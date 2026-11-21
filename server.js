import express from 'express';
import http from 'node:http';
import crypto from 'node:crypto';
import WebSocket, { WebSocketServer } from 'ws';

const PORT = Number(process.env.PORT) || 3000;
const GEMINI_API_KEY = String(process.env.GEMINI_API_KEY || '').trim();
const STREAM_SECRET = String(process.env.STREAM_SECRET || '').trim();
const GEMINI_LIVE_MODEL = String(process.env.GEMINI_LIVE_MODEL || 'gemini-3.8-live').trim();
const GEMINI_VOICE = String(process.env.GEMINI_VOICE || 'Kore').trim();
const SYSTEM_INSTRUCTION = String(process.env.SYSTEM_INSTRUCTION || 'אתה עוזר קולי בעברית לשיחות טלפון. דבר בעברית טבעית, ובצורה מכובדת, אם נדרש להאריך בתשובה אל תימנע מלעשות זאת.').trim();
const BARGE_IN_RMS = Number(process.env.BARGE_IN_RMS) || 800;
const BARGE_IN_FRAMES = Math.max(1, Number(process.env.BARGE_IN_FRAMES) || 3);
const PBX_AUDIO_FRAME_BYTES = 320;
const PBX_AUDIO_FRAME_MS = 20;
const PBX_AUDIO_BYTES_PER_SECOND = 8000 * 2;
const MAX_PBX_AUDIO_QUEUE_SECONDS = 30;
const MAX_PBX_AUDIO_QUEUE_BYTES = MAX_PBX_AUDIO_QUEUE_SECONDS * PBX_AUDIO_BYTES_PER_SECOND;
const OPENING_GREETING = 'שלום, איך אוכל לעזור לך היום?';

function pcm8kTo16k(buffer) {
  if (!buffer.length) return Buffer.alloc(0);
  const out = Buffer.alloc(buffer.length * 2);
  for (let i = 0; i < buffer.length - 1; i += 2) {
    const sample = buffer.readInt16LE(i);
    out.writeInt16LE(sample, i * 2);
    out.writeInt16LE(sample, i * 2 + 2);
  }
  return out;
}

function pcm24kTo8k(buffer) {
  if (!buffer.length) return Buffer.alloc(0);
  const frame = 6;
  const count = Math.floor(buffer.length / frame);
  const out = Buffer.alloc(count * 2);
  for (let i = 0; i < count; i++) {
    let sum = 0;
    for (let j = 0; j < 3; j++) sum += buffer.readInt16LE(i * frame + j * 2);
    out.writeInt16LE(Math.round(sum / 3), i * 2);
  }
  return out;
}

function pcmRms(buffer) {
  const samples = Math.floor(buffer.length / 2);
  if (!samples) return 0;
  let sum = 0;
  for (let i = 0; i < samples; i++) {
    const sample = buffer.readInt16LE(i * 2);
    sum += sample * sample;
  }
  return Math.sqrt(sum / samples);
}

const app = express();
app.get('/', (_req, res) => res.type('text/plain').send('ok'));
app.get('/health', (_req, res) => res.status(200).json({ ok: true, geminiConfigured: !!GEMINI_API_KEY }));

const server = http.createServer(app);
const wss = new WebSocketServer({ noServer: true, maxPayload: 512 * 1024 });

server.on('upgrade', (req, socket, head) => {
  if (req.url !== '/voice') {
    socket.destroy();
    return;
  }

  const authorization = String(req.headers.authorization || '').trim();
  if (STREAM_SECRET && authorization !== `Bearer ${STREAM_SECRET}`) {
    socket.write('HTTP/1.1 401 Unauthorized\r\nConnection: close\r\n\r\n');
    socket.destroy();
    return;
  }

  wss.handleUpgrade(req, socket, head, ws => wss.emit('connection', ws, req));
});

wss.on('connection', ws => {
  const call = { callId: '', caller: '', system: '', token: '', closing: false, startedAt: 0 };
  let gemini = null;
  let geminiReady = false;
  let callerAudioFrames = 0;
  let geminiResponseAudioChunks = 0;
  let geminiResponseAudioBytes = 0;
  let geminiResponseAudioRate = 24000;
  let droppingAssistantAudio = false;
  let aiSpeaking = false;
  let loudFrames = 0;
  const pbxAudioQueue = [];
  let pbxAudioQueueOffset = 0;
  let pbxAudioQueueBytes = 0;
  let pbxAudioQueuePeakBytes = 0;
  let pbxPlaybackTimer = null;

  const closeGemini = () => {
    if (gemini && gemini.readyState === WebSocket.OPEN) gemini.close();
  };

  const sendPBX = payload => {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(payload));
    }
  };

  const sendClear = reason => {
    const hadPlayback = aiSpeaking || pbxAudioQueueBytes > 0;
    if (!hadPlayback) return;
    aiSpeaking = false;
    pbxAudioQueue.length = 0;
    pbxAudioQueueOffset = 0;
    pbxAudioQueueBytes = 0;
    pbxAudioQueuePeakBytes = 0;
    pbxWsBufferedPeakBytes = 0;
    if (pbxPlaybackTimer) {
      clearInterval(pbxPlaybackTimer);
      pbxPlaybackTimer = null;
    }
    console.log(call.callId, 'clearing playback:', reason);
    sendPBX({ type: 'clear' });
  };

  const pumpPbxAudio = () => {
    if (ws.readyState !== WebSocket.OPEN) {
      if (pbxPlaybackTimer) clearInterval(pbxPlaybackTimer);
      pbxPlaybackTimer = null;
      return;
    }
    if (pbxAudioQueueBytes === 0) {
      if (pbxPlaybackTimer) clearInterval(pbxPlaybackTimer);
      pbxPlaybackTimer = null;
      aiSpeaking = false;
      pbxAudioFlushPending = false;
      console.log(
        call.callId,
        'PBX audio playback queue drained; peak queued seconds:',
        (pbxAudioQueuePeakBytes / PBX_AUDIO_BYTES_PER_SECOND).toFixed(2),
        'PBX WebSocket buffered bytes:',
        ws.bufferedAmount
      );
      pbxAudioQueuePeakBytes = 0;
      pbxWsBufferedPeakBytes = 0;
      return;
    }

    const frameLength = Math.min(PBX_AUDIO_FRAME_BYTES, pbxAudioQueueBytes);
    if (frameLength < PBX_AUDIO_FRAME_BYTES && !pbxAudioFlushPending) return;
    const frame = Buffer.alloc(frameLength);
    let written = 0;
    while (written < frameLength) {
      const current = pbxAudioQueue[0];
      const available = current.length - pbxAudioQueueOffset;
      const take = Math.min(available, frameLength - written);
      current.copy(frame, written, pbxAudioQueueOffset, pbxAudioQueueOffset + take);
      written += take;
      pbxAudioQueueOffset += take;
      pbxAudioQueueBytes -= take;
      if (pbxAudioQueueOffset === current.length) {
        pbxAudioQueue.shift();
        pbxAudioQueueOffset = 0;
      }
    }
    ws.send(frame, { binary: true });
    pbxWsBufferedPeakBytes = Math.max(pbxWsBufferedPeakBytes, ws.bufferedAmount);
  };

  let pbxAudioFlushPending = false;
  let pbxWsBufferedPeakBytes = 0;
  const enqueuePbxAudio = audio => {
    if (!audio.length) return;
    if (pbxAudioQueueBytes + audio.length > MAX_PBX_AUDIO_QUEUE_BYTES) {
      console.error(
        call.callId,
        'PBX audio queue limit exceeded; stopping current response audio and keeping call open; queued seconds:',
        (pbxAudioQueueBytes / PBX_AUDIO_BYTES_PER_SECOND).toFixed(2),
        'incoming seconds:',
        (audio.length / PBX_AUDIO_BYTES_PER_SECOND).toFixed(2),
        'limit seconds:',
        MAX_PBX_AUDIO_QUEUE_SECONDS,
        'PBX WebSocket buffered bytes:',
        ws.bufferedAmount,
        'peak PBX WebSocket buffered bytes:',
        pbxWsBufferedPeakBytes
      );
      droppingAssistantAudio = true;
      sendClear('audio queue limit exceeded; current response will be partial');
      return;
    }
    pbxAudioQueue.push(audio);
    pbxAudioQueueBytes += audio.length;
    pbxAudioQueuePeakBytes = Math.max(pbxAudioQueuePeakBytes, pbxAudioQueueBytes);
    pbxAudioFlushPending = false;
    aiSpeaking = true;
    if (!pbxPlaybackTimer) pbxPlaybackTimer = setInterval(pumpPbxAudio, PBX_AUDIO_FRAME_MS);
  };

  const sendAudioToGemini = audio => {
    if (!geminiReady || !gemini || gemini.readyState !== WebSocket.OPEN) return;
    const pcm16k = pcm8kTo16k(audio);
    gemini.send(JSON.stringify({
      realtimeInput: {
        audio: { mimeType: 'audio/pcm;rate=16000', data: pcm16k.toString('base64') }
      }
    }));
  };

  const openGemini = () => {
    if (!GEMINI_API_KEY) {
      console.error('GEMINI_API_KEY is missing');
      ws.close(1011, 'AI service not configured');
      return;
    }

    const endpoint = `wss://generativelanguage.googleapis.com/ws/google.ai.generativelanguage.v1alpha.GenerativeService.BidiGenerateContent?key=${encodeURIComponent(GEMINI_API_KEY)}`;
    gemini = new WebSocket(endpoint);

    gemini.on('open', () => {
      console.log(call.callId, 'Gemini WebSocket opened; sending setup for', GEMINI_LIVE_MODEL);
      gemini.send(JSON.stringify({
        setup: {
          model: `models/${GEMINI_LIVE_MODEL}`,
          generationConfig: {
            responseModalities: ['AUDIO'],
            speechConfig: {
              voiceConfig: {
                prebuiltVoiceConfig: { voiceName: GEMINI_VOICE }
              }
            }
          },
          systemInstruction: { parts: [{ text: SYSTEM_INSTRUCTION }] }
        }
      }));
    });

    gemini.on('message', raw => {
      let message;
      try { message = JSON.parse(raw.toString()); } catch { return; }

      if (message.setupComplete) {
        geminiReady = true;
        console.log(call.callId, 'Gemini setup complete');
        gemini.send(JSON.stringify({
          clientContent: {
            turns: [{ role: 'user', parts: [{ text: `פתח את השיחה ואמור בדיוק: "${OPENING_GREETING}"` }] }],
            turnComplete: true
          }
        }));
        return;
      }

      const serverContent = message.serverContent;
      if (serverContent?.interrupted) {
        console.log(
          call.callId,
          'Gemini reported interrupted response; audio chunks generated:',
          geminiResponseAudioChunks,
          'audio bytes generated:',
          geminiResponseAudioBytes
        );
        geminiResponseAudioChunks = 0;
        geminiResponseAudioBytes = 0;
        geminiResponseAudioRate = 24000;
        droppingAssistantAudio = false;
        sendClear('Gemini detected caller interruption');
      }

      const parts = serverContent?.modelTurn?.parts || [];
      for (const part of parts) {
        if (droppingAssistantAudio) continue;
        const inline = part.inlineData || part.inline_data;
        const base64 = inline?.data;
        if (!base64) continue;
        const pcm = Buffer.from(base64, 'base64');
        if (geminiResponseAudioChunks === 0) {
          console.log(call.callId, 'Gemini response audio started');
        }
        geminiResponseAudioChunks++;
        geminiResponseAudioBytes += pcm.length;
        const pcmMime = inline.mimeType || inline.mime_type || '';
        const outputRate = Number(/rate=(\d+)/i.exec(pcmMime)?.[1]) || 24000;
        geminiResponseAudioRate = outputRate;
        const pcm8k = outputRate === 8000 ? pcm : pcm24kTo8k(pcm);
        if (ws.readyState === WebSocket.OPEN && pcm8k.length) {
          enqueuePbxAudio(pcm8k);
        }
      }

      if (serverContent?.turnComplete) {
        if (droppingAssistantAudio) {
          console.warn(call.callId, 'Gemini response audio was stopped after queue limit; call remains open');
          droppingAssistantAudio = false;
        }
        console.log(
          call.callId,
          'Gemini turn complete; audio chunks:',
          geminiResponseAudioChunks,
          'audio seconds:',
          (geminiResponseAudioBytes / (geminiResponseAudioRate * 2)).toFixed(2)
        );
        geminiResponseAudioChunks = 0;
        geminiResponseAudioBytes = 0;
        geminiResponseAudioRate = 24000;
        pbxAudioFlushPending = true;
        if (!pbxPlaybackTimer && pbxAudioQueueBytes > 0) {
          pbxPlaybackTimer = setInterval(pumpPbxAudio, PBX_AUDIO_FRAME_MS);
        }
      }

      if (message.error) {
        console.error(call.callId, 'gemini live error', message.error);
      }
    });

    gemini.on('close', (code, reason) => {
      geminiReady = false;
      const durationMs = call.startedAt ? Date.now() - call.startedAt : 0;
      console.log(call.callId, 'gemini live closed', code, reason.toString(), 'PBX state:', ws.readyState, 'call closing:', call.closing, 'duration ms:', durationMs);
      if (ws.readyState === WebSocket.OPEN && !call.closing) ws.close(1011, 'AI session closed');
    });

    gemini.on('error', error => {
      console.error(call.callId, 'gemini live websocket error', error.message);
    });
  };

  ws.on('message', (data, isBinary) => {
    if (isBinary) {
      const audio = Buffer.isBuffer(data) ? data : Buffer.from(data);
      if (audio.length < 2) return;
      const rms = pcmRms(audio);
      callerAudioFrames++;
      if (callerAudioFrames === 1 || callerAudioFrames % 250 === 0) {
        console.log(
          call.callId,
          'caller audio frames received:',
          callerAudioFrames,
          'latest frame bytes:',
          audio.length,
          'RMS:',
          Math.round(rms),
          'sent to Gemini:',
          geminiReady
        );
      }

      if (aiSpeaking) {
        loudFrames = rms >= BARGE_IN_RMS ? loudFrames + 1 : 0;
        if (loudFrames >= BARGE_IN_FRAMES) {
          sendClear('caller speech detected');
          loudFrames = 0;
        }
      } else {
        loudFrames = 0;
      }

      if (geminiReady) {
        sendAudioToGemini(audio);
      }
      return;
    }

    let message;
    try { message = JSON.parse(data.toString()); } catch (error) {
      console.error(call.callId, 'invalid JSON from PBX', error.message);
      return;
    }

    switch (message.type) {
      case 'start':
        Object.assign(call, {
          callId: String(message.callId || ''),
          caller: String(message.caller || ''),
          system: String(message.system || ''),
          token: String(message.token || ''),
          startedAt: Date.now()
        });
        console.log(call.callId, 'PBX streaming call started', call.caller, call.system);
        openGemini();
        break;

      case 'dtmf':
        if (!geminiReady || !gemini || gemini.readyState !== WebSocket.OPEN) break;
        gemini.send(JSON.stringify({ clientContent: { turns: [{ role: 'user', parts: [{ text: `המתקשר הקיש את המקש ${String(message.digit || '').slice(0, 1)}.` }] }], turnComplete: true } }));
        break;

      case 'hangup':
        console.log(call.callId, 'PBX hangup received');
        call.closing = true;
        closeGemini();
        if (ws.readyState === WebSocket.OPEN) ws.close();
        break;

      default:
        console.log(call.callId, 'PBX event', message);
    }
  });

  ws.on('close', (code, reason) => {
    call.closing = true;
    const durationMs = call.startedAt ? Date.now() - call.startedAt : 0;
    console.log(call.callId, 'PBX websocket closed', code, reason.toString(), 'duration ms:', durationMs);
    closeGemini();
  });

  ws.on('error', error => console.error(call.callId, 'PBX websocket error', error.message));

  // Random local call identifier only as a fallback before PBX sends start.
  call.callId = crypto.randomUUID();
});

server.listen(PORT, () => {
  console.log(`Kav AI streaming server listening on port ${PORT}`);
});
