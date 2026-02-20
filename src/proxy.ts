import WebSocket from "ws";
import http from "http";
import express, { Request, Response } from "express";
import { proxyConfig } from "./config";
import { GladiaClient } from "./gladia";
import { createLogger } from "./utils";
import { AudioVisualizer, getTUI } from "./audioVisualizer";
import { getProcessLogger } from "./processLogger";
import * as fs from "fs";
import * as path from "path";
import type { MeetingBaasWebhookEvent } from "./webhookHandler";

const logger = createLogger("Proxy");

// Speaker module loaded dynamically in initializeSpeaker() if playback is enabled

// Define simple message types to replace protobufs
interface AudioMessage {
  type: "audio";
  data: {
    audio: string; // Base64 encoded audio
    sampleRate: number;
    channels: number;
  };
}

interface TranscriptionMessage {
  type: "transcription";
  data: {
    text: string;
    isFinal: boolean;
    startTime: number;
    endTime: number;
  };
}

interface TextMessage {
  type: "text";
  data: {
    text: string;
  };
}

interface SpeakerInfo {
  name: string;
  id: number;
  timestamp: number;
  isSpeaking: boolean;
}

type Message = AudioMessage | TranscriptionMessage | TextMessage;

// Helper function to safely inspect message content
function inspectMessage(message: Buffer | string | unknown): string {
  try {
    // If it's a buffer, convert to string for inspection
    if (Buffer.isBuffer(message)) {
      // Try to parse as JSON first
      try {
        const jsonStr = message.toString("utf8");
        const json = JSON.parse(jsonStr);
        return `[Buffer as JSON] ${JSON.stringify(json, null, 2)}`;
      } catch {
        // If not JSON, show as hex if it's binary-looking, or as string if not
        const str = message.toString("utf8");
        if (/[\x00-\x08\x0E-\x1F\x80-\xFF]/.test(str)) {
          // Likely binary data, show first 100 bytes as hex
          return `[Binary Buffer] ${message.slice(0, 100).toString("hex")}${
            message.length > 100 ? "..." : ""
          }`;
        } else {
          // Printable string
          return `[String Buffer] ${str.slice(0, 500)}${
            str.length > 500 ? "..." : ""
          }`;
        }
      }
    }

    // If it's already a string
    if (typeof message === "string") {
      // Try to parse as JSON
      try {
        const json = JSON.parse(message);
        return `[String as JSON] ${JSON.stringify(json, null, 2)}`;
      } catch {
        // Plain string
        return `[String] ${message.slice(0, 500)}${
          message.length > 500 ? "..." : ""
        }`;
      }
    }

    // For any other type
    return `[${typeof message}] ${JSON.stringify(message, null, 2)}`;
  } catch (error) {
    return `[Inspection Error] Failed to inspect message: ${error}`;
  }
}

class TranscriptionProxy {
  private app: express.Application;
  private httpServer: http.Server;
  private server: WebSocket.Server;
  private botClient: WebSocket | null = null;
  private meetingBaasClients: Set<WebSocket> = new Set();
  private gladiaClient: GladiaClient | null = null;
  private isGladiaSessionActive: boolean = false;
  private transcriptionEnabled: boolean;
  private lastSpeaker: string | null = null;
  private audioBuffers: Buffer[] = [];
  private recordingStartTime: number | null = null;
  private audioVisualizer: AudioVisualizer;
  private speaker: any = null;
  private isPlaybackReady: boolean = false;
  private lastRecordingPath: string | null = null;
  private lastRecordingSize: number = 0;
  private meetingBaasHandlers: Map<
    string,
    (event: MeetingBaasWebhookEvent) => void | Promise<void>
  > = new Map();
  private waitingForRecordingStatus: boolean = true;
  private transcriptionInitialized: boolean = false;
  private mode: string;

  // Latency tracking
  private connectionTime: number | null = null;
  private firstAudioTime: number | null = null;
  private lastChunkTime: number | null = null;
  private chunkCount: number = 0;
  private totalBytes: number = 0;
  private interChunkDelays: number[] = [];
  private latencyLogInterval: NodeJS.Timeout | null = null;

  // Audio quality tracking
  private clippedSamples: number = 0;
  private totalSamples: number = 0;
  private peakAmplitude: number = 0;
  private silentChunks: number = 0;
  private gapCount: number = 0; // chunks arriving >100ms late (causes crackling)
  private speakerBackpressureCount: number = 0;
  private chunkSizes: number[] = [];

  constructor(mode: string = "Proxy") {
    this.mode = mode;

    // In local mode, don't wait for recording status webhook - start transcription immediately
    // In remote mode, wait for bot.status_change webhook with in_call_not_recording status
    if (mode === "Local") {
      this.waitingForRecordingStatus = false;
      logger.info("Local mode: will start transcription immediately on connection");
    } else {
      this.waitingForRecordingStatus = true;
      logger.info("Remote mode: will wait for in_call_not_recording status before starting transcription");
    }

    // Create Express app for HTTP endpoints
    this.app = express();
    this.app.use(express.json());

    // Setup webhook routes
    this.setupWebhookRoutes();

    // Create HTTP server
    this.httpServer = http.createServer(this.app);

    // Attach WebSocket server to HTTP server
    this.server = new WebSocket.Server({
      server: this.httpServer,
    });

    // Start listening
    this.httpServer.listen(proxyConfig.port, proxyConfig.host, () => {
      logger.info(`Proxy server started on ${proxyConfig.host}:${proxyConfig.port}`);
      logger.info(`WebSocket endpoint: ws://${proxyConfig.host}:${proxyConfig.port}`);
      logger.info(`Webhook endpoint: http://${proxyConfig.host}:${proxyConfig.port}/webhooks/meetingbaas`);
    });

    this.transcriptionEnabled = proxyConfig.transcription.enabled;

    if (this.transcriptionEnabled) {
      this.gladiaClient = new GladiaClient();
    } else {
      logger.info("Transcription DISABLED - running in echo/playback only mode");
    }

    // Use the TUI singleton (initialized in index.ts) and update its config
    const tui = getTUI();
    if (tui) {
      tui.setConfig(mode, proxyConfig.port);
      this.audioVisualizer = tui;
    } else {
      // Fallback: create new instance if TUI wasn't initialized (shouldn't happen)
      this.audioVisualizer = new AudioVisualizer(mode, proxyConfig.port);
    }

    // Initialize speaker for audio playback if enabled
    if (proxyConfig.playback.enabled) {
      this.initializeSpeaker();
    }

    // Set up transcription callback
    if (this.gladiaClient) {
      this.gladiaClient.onTranscription((text, isFinal) => {
        // Show transcription in visualizer
        this.audioVisualizer.showTranscription(text, isFinal);

        // Create a transcription message to send to the bot client
        const transcriptionMsg = {
          type: "transcription",
          data: {
            text: text,
            isFinal: isFinal,
            startTime: Date.now(), // Approximate
            endTime: Date.now(), // Approximate
          },
        };

        // Send the transcription to the bot client
        if (this.botClient && this.botClient.readyState === WebSocket.OPEN) {
          this.botClient.send(JSON.stringify(transcriptionMsg));
        }
      });
    }

    this.server.on("connection", (ws) => {
      logger.info("New connection established");

      // Determine if this is a bot or MeetingBaas client
      ws.once("message", (message) => {
        try {
          const msg = JSON.parse(message.toString());
          if (msg.type === "register" && msg.client === "bot") {
            this.setupBotClient(ws);
          } else {
            this.setupMeetingBaasClient(ws);
          }
        } catch (error) {
          // If message is not valid JSON, assume it's a MeetingBaas client
          this.setupMeetingBaasClient(ws);
        }
      });
    });
  }

  /**
   * Get latency and streaming statistics
   */
  private getLatencyStats() {
    const now = Date.now();
    const elapsed = this.connectionTime ? (now - this.connectionTime) / 1000 : 0;
    const delays = this.interChunkDelays;
    const avgDelay = delays.length > 0 ? delays.reduce((a, b) => a + b, 0) / delays.length : 0;
    const minDelay = delays.length > 0 ? Math.min(...delays) : 0;
    const maxDelay = delays.length > 0 ? Math.max(...delays) : 0;
    // Jitter = standard deviation of inter-chunk delays
    const variance = delays.length > 0
      ? delays.reduce((sum, d) => sum + Math.pow(d - avgDelay, 2), 0) / delays.length
      : 0;
    const jitter = Math.sqrt(variance);

    return {
      connected: !!this.connectionTime,
      elapsedSeconds: Math.round(elapsed),
      timeToFirstAudioMs: this.firstAudioTime && this.connectionTime
        ? this.firstAudioTime - this.connectionTime : null,
      chunkCount: this.chunkCount,
      totalBytes: this.totalBytes,
      totalMB: (this.totalBytes / 1024 / 1024).toFixed(2),
      dataRateKbps: elapsed > 0 ? (this.totalBytes * 8) / 1000 / elapsed : 0,
      avgInterChunkMs: avgDelay,
      minInterChunkMs: minDelay,
      maxInterChunkMs: maxDelay,
      jitterMs: jitter,
      // Expected chunk interval for 16kHz mono 16-bit = 32000 bytes/sec
      expectedChunkIntervalMs: this.chunkCount > 0
        ? (this.totalBytes / this.chunkCount) / 32 // 32 bytes per ms at 16kHz 16-bit mono
        : 0,
      // Audio quality
      audio: {
        peakAmplitude: this.peakAmplitude,
        peakDb: this.peakAmplitude > 0 ? (20 * Math.log10(this.peakAmplitude / 32767)).toFixed(1) : "-inf",
        clippedSamples: this.clippedSamples,
        clippingPercent: this.totalSamples > 0 ? ((this.clippedSamples / this.totalSamples) * 100).toFixed(4) : "0",
        totalSamples: this.totalSamples,
        silentChunks: this.silentChunks,
        silentPercent: this.chunkCount > 0 ? ((this.silentChunks / this.chunkCount) * 100).toFixed(1) : "0",
        gapCount: this.gapCount, // gaps >100ms = crackling source
        speakerBackpressure: this.speakerBackpressureCount,
        avgChunkSize: this.chunkSizes.length > 0 ? Math.round(this.chunkSizes.reduce((a, b) => a + b, 0) / this.chunkSizes.length) : 0,
        minChunkSize: this.chunkSizes.length > 0 ? Math.min(...this.chunkSizes) : 0,
        maxChunkSize: this.chunkSizes.length > 0 ? Math.max(...this.chunkSizes) : 0,
      },
      // Diagnostic: likely cause of crackling
      diagnosis: this.getDiagnosis(),
    };
  }

  /**
   * Diagnose likely cause of audio crackling
   */
  private getDiagnosis(): string[] {
    const issues: string[] = [];
    if (this.gapCount > 0) {
      issues.push(`${this.gapCount} network gaps >100ms detected - main crackling source (network jitter via ngrok)`);
    }
    if (this.speakerBackpressureCount > 0) {
      issues.push(`${this.speakerBackpressureCount}x speaker backpressure - audio arriving faster than playback`);
    }
    if (this.clippedSamples > 0) {
      const pct = ((this.clippedSamples / this.totalSamples) * 100).toFixed(2);
      issues.push(`${pct}% samples clipped (amplitude at max 32767) - digital distortion`);
    }
    const delays = this.interChunkDelays;
    if (delays.length > 10) {
      const avgDelay = delays.reduce((a, b) => a + b, 0) / delays.length;
      const variance = delays.reduce((sum, d) => sum + Math.pow(d - avgDelay, 2), 0) / delays.length;
      const jitter = Math.sqrt(variance);
      if (jitter > 20) {
        issues.push(`High jitter: ${jitter.toFixed(1)}ms - inconsistent chunk delivery`);
      }
    }
    if (this.chunkSizes.length > 1) {
      const sizes = new Set(this.chunkSizes);
      if (sizes.size > 5) {
        issues.push(`Inconsistent chunk sizes (${sizes.size} different sizes) - may cause playback glitches`);
      }
    }
    if (issues.length === 0) {
      issues.push("No obvious issues detected");
    }
    return issues;
  }

  /**
   * Setup webhook routes for MeetingBaas events
   */
  private setupWebhookRoutes(): void {
    // Health check endpoint
    this.app.get("/health", (req: Request, res: Response) => {
      res.status(200).json({
        status: "healthy",
        service: "transcription-proxy",
        timestamp: new Date().toISOString(),
      });
    });

    // Latency stats endpoint
    this.app.get("/stats", (req: Request, res: Response) => {
      res.status(200).json(this.getLatencyStats());
    });

    // MeetingBaas webhook endpoint
    this.app.post("/webhooks/meetingbaas", async (req: Request, res: Response) => {
      try {
        logger.info("Received MeetingBaas webhook");
        getProcessLogger()?.info("Received MeetingBaas webhook", "Proxy", { body: req.body });

        const event = req.body as MeetingBaasWebhookEvent;

        if (!event.event) {
          logger.error("Invalid MeetingBaas webhook: missing event type");
          return res.status(400).json({
            error: "Invalid webhook payload",
            details: "Missing event type",
          });
        }

        // Process the webhook event
        await this.processMeetingBaasWebhook(event);

        res.status(200).json({
          received: true,
          event: event.event,
        });
      } catch (error: any) {
        logger.error("Error processing MeetingBaas webhook:", error.message);
        res.status(500).json({
          error: "Internal server error",
          details: error.message,
        });
      }
    });
  }

  /**
   * Process MeetingBaas webhook events
   */
  private async processMeetingBaasWebhook(event: MeetingBaasWebhookEvent): Promise<void> {
    const { event: eventType, data } = event;

    logger.info(`MeetingBaas event: ${eventType}`);
    getProcessLogger()?.info(`Processing MeetingBaas event: ${eventType}`, "Proxy", { data });

    // Add to visualizer logs
    this.audioVisualizer.addLog(`MeetingBaas: ${eventType}`, "info");

    switch (eventType) {
      case "bot.joining":
        logger.info("Bot is joining the meeting...");
        break;

      case "bot.in_waiting_room":
        logger.info("Bot is in the waiting room");
        this.audioVisualizer.addLog("Bot in waiting room", "info");
        break;

      case "bot.joined":
        logger.info(`Bot joined meeting: ${data?.meeting_url || "unknown"}`);
        this.audioVisualizer.addLog("Bot joined meeting", "info");
        break;

      case "bot.left":
        logger.info("Bot left the meeting");
        this.audioVisualizer.addLog("Bot left meeting", "info");
        break;

      case "bot.recording_permission_allowed":
        logger.info("Recording permission granted");
        this.audioVisualizer.addLog("Recording permission granted", "info");
        break;

      case "bot.recording_permission_denied":
        logger.warn("Recording permission denied");
        this.audioVisualizer.addLog("Recording permission DENIED", "error");
        break;

      case "recording.started":
        logger.info("Recording started");
        this.audioVisualizer.addLog("Recording started", "info");
        break;

      case "recording.ready":
        logger.info(`Recording ready: ${data?.recording_url || "URL not available"}`);
        this.audioVisualizer.addLog(`Recording ready`, "info");
        break;

      case "recording.failed":
        logger.error(`Recording failed: ${data?.error || "unknown error"}`);
        this.audioVisualizer.addLog(`Recording failed: ${data?.error}`, "error");
        break;

      case "transcription.ready":
        logger.info("Async transcription ready!");
        if (data?.transcript?.text) {
          const preview = data.transcript.text.substring(0, 100);
          logger.info(`Transcript preview: ${preview}...`);
        }
        if (data?.transcript_url) {
          logger.info(`Transcript URL: ${data.transcript_url}`);
        }
        this.audioVisualizer.addLog("Async transcription ready!", "info");
        break;

      case "transcription.failed":
        logger.error(`Async transcription failed: ${data?.error || "unknown error"}`);
        this.audioVisualizer.addLog(`Transcription failed: ${data?.error}`, "error");
        break;

      case "meeting.ended":
        logger.info("Meeting has ended");
        this.audioVisualizer.addLog("Meeting ended", "info");
        break;

      case "bot.status_change":
        // status can be a string or an object with code property
        const statusObj = data?.status;
        const statusCode = typeof statusObj === "object" && statusObj !== null ? statusObj.code : statusObj;
        logger.info(`Bot status: ${statusCode}`);
        this.audioVisualizer.addLog(`Bot: ${statusCode}`, "info");

        // Handle specific status codes
        switch (statusCode) {
          case "joining_call":
            this.audioVisualizer.addLog("Bot joining call...", "info");
            break;
          case "in_waiting_room":
            this.audioVisualizer.addLog("Bot in waiting room", "info");
            break;
          case "in_call_not_recording":
            this.audioVisualizer.addLog("Bot in call (audio connected)", "info");
            // Audio is connected - start transcription now to not miss any audio
            if (this.waitingForRecordingStatus && !this.transcriptionInitialized) {
              logger.info("🎙️ Bot audio connected - starting transcription session");
              this.audioVisualizer.addLog("Audio connected - starting transcription", "info");
              this.waitingForRecordingStatus = false;
              this.initializeTranscriptionSession();
            }
            break;
          case "in_call_recording":
            this.audioVisualizer.addLog("Bot recording started!", "info");
            break;
          case "call_ended":
            this.audioVisualizer.addLog("Call ended", "info");
            break;
          default:
            this.audioVisualizer.addLog(`Bot status: ${statusCode}`, "info");
        }
        break;

      default:
        logger.info(`MeetingBaas event: ${eventType}`);
        this.audioVisualizer.addLog(`MeetingBaas: ${eventType}`, "info");
        if (data) {
          logger.info(`Event data: ${JSON.stringify(data, null, 2)}`);
        }
    }

    // Call registered handlers
    const handler = this.meetingBaasHandlers.get(eventType);
    if (handler) {
      try {
        await handler(event);
      } catch (error: any) {
        logger.error(`Error in MeetingBaas handler for ${eventType}:`, error.message);
      }
    }

    // Call wildcard handler
    const wildcardHandler = this.meetingBaasHandlers.get("*");
    if (wildcardHandler) {
      try {
        await wildcardHandler(event);
      } catch (error: any) {
        logger.error("Error in MeetingBaas wildcard handler:", error.message);
      }
    }
  }

  /**
   * Register a handler for MeetingBaas webhook events
   */
  public onMeetingBaas(
    eventType: string,
    handler: (event: MeetingBaasWebhookEvent) => void | Promise<void>
  ): void {
    this.meetingBaasHandlers.set(eventType, handler);
    logger.info(`Registered MeetingBaas handler for: ${eventType}`);
  }

  /**
   * Initialize the speaker for audio playback
   */
  private initializeSpeaker(): void {
    try {
      const Speaker = require("speaker");
      this.speaker = new Speaker({
        channels: proxyConfig.audioParams.channels,
        bitDepth: 16,
        sampleRate: proxyConfig.audioParams.sampleRate,
        highWaterMark: 0,
        lowWaterMark: 0,
      });

      logger.info(`Speaker configured: ${proxyConfig.audioParams.sampleRate}Hz, ${proxyConfig.audioParams.channels}ch, 16-bit`);

      // Mark as ready immediately
      this.isPlaybackReady = true;

      this.speaker.on("open", () => {
        logger.info("Audio playback started");
        this.isPlaybackReady = true;
      });

      this.speaker.on("error", (error: any) => {
        logger.error("Speaker error:", error);
        this.speaker = null;
        this.isPlaybackReady = false;
      });

      this.speaker.on("close", () => {
        logger.info("Audio playback stopped");
        this.isPlaybackReady = false;
      });

      logger.info("Speaker initialized for audio playback");
    } catch (error) {
      logger.error("Failed to initialize speaker:", error);
      this.speaker = null;
    }
  }

  /**
   * Analyze a 16-bit PCM audio chunk for quality issues
   */
  private analyzeAudioChunk(audioBuffer: Buffer): void {
    let chunkPeak = 0;
    let chunkRms = 0;
    let clipped = 0;
    const numSamples = Math.floor(audioBuffer.length / 2); // 16-bit = 2 bytes per sample

    for (let i = 0; i < audioBuffer.length - 1; i += 2) {
      const sample = audioBuffer.readInt16LE(i);
      const abs = Math.abs(sample);
      chunkRms += sample * sample;
      if (abs > chunkPeak) chunkPeak = abs;
      if (abs >= 32767) clipped++; // Max value for 16-bit = clipping
    }

    this.totalSamples += numSamples;
    this.clippedSamples += clipped;
    if (chunkPeak > this.peakAmplitude) this.peakAmplitude = chunkPeak;

    // Detect silent chunks (RMS < -60dB)
    const rms = numSamples > 0 ? Math.sqrt(chunkRms / numSamples) : 0;
    if (rms < 100) this.silentChunks++; // ~-50dB threshold
  }

  /**
   * Play audio directly to speaker
   */
  private playAudio(audioBuffer: Buffer): void {
    if (!proxyConfig.playback.enabled || !this.speaker || !this.isPlaybackReady) {
      return;
    }

    const canWrite = this.speaker.write(audioBuffer);
    if (!canWrite) {
      this.speakerBackpressureCount++;
    }
  }

  private setupBotClient(ws: WebSocket) {
    logger.info("Bot client connected");
    this.botClient = ws;

    ws.on("message", (message) => {
      // Log all messages from bot
      logger.info(`Message from bot: ${inspectMessage(message)}`);

      // Forward bot messages to all MeetingBaas clients
      this.meetingBaasClients.forEach((client) => {
        if (client.readyState === WebSocket.OPEN) {
          client.send(message.toString());
        }
      });
    });

    ws.on("close", () => {
      logger.info("Bot client disconnected");
      this.botClient = null;
    });

    ws.on("error", (error) => {
      logger.error("Bot client error:", error);
      this.audioVisualizer?.showError(`Bot client error: ${error.message || error}`);
    });
  }

  private setupMeetingBaasClient(ws: WebSocket) {
    logger.info("MeetingBaas client connected");
    this.connectionTime = Date.now();
    this.meetingBaasClients.add(ws);

    // Start periodic latency logging
    this.latencyLogInterval = setInterval(() => {
      if (this.chunkCount > 0) {
        const stats = this.getLatencyStats();
        logger.info(`📊 Streaming stats: ${stats.chunkCount} chunks, ${stats.dataRateKbps.toFixed(1)} kbps, avg gap: ${stats.avgInterChunkMs.toFixed(1)}ms, jitter: ${stats.jitterMs.toFixed(1)}ms`);
      }
    }, 10000);

    // Don't initialize transcription immediately - wait for in_call_not_recording status via webhook
    if (this.waitingForRecordingStatus) {
      logger.info("⏳ Waiting for bot to reach 'in_call_not_recording' status before starting transcription...");
      this.audioVisualizer.addLog("Waiting for recording status...", "info");
    } else {
      // If not waiting (e.g., local mode without webhooks), initialize immediately
      this.initializeTranscriptionSession();
    }

    // Set recording start time if recording is enabled
    if (proxyConfig.recording.enabled && this.recordingStartTime === null) {
      this.recordingStartTime = Date.now();
      logger.info("Audio recording started");
    }

    ws.on("message", (message) => {
      // Skip logging binary buffers and try to transcribe them
      if (Buffer.isBuffer(message)) {
        // Try to identify if it's audio data
        try {
          const jsonStr = message.toString("utf8");
          const jsonData = JSON.parse(jsonStr);

          // If it's speaker information
          if (
            Array.isArray(jsonData) &&
            jsonData.length > 0 &&
            "name" in jsonData[0] &&
            "isSpeaking" in jsonData[0]
          ) {
            const speakerInfo = jsonData[0] as SpeakerInfo;

            // Only log when a new speaker starts talking (different from the last one)
            // or when we haven't seen any speaker yet
            if (
              speakerInfo.isSpeaking &&
              (this.lastSpeaker === null ||
                this.lastSpeaker !== speakerInfo.name)
            ) {
              // Update our last speaker tracking
              this.lastSpeaker = speakerInfo.name;

              // Update visualizer with new speaker
              this.audioVisualizer.updateSpeaker(speakerInfo.name);

              // Log the new speaker
              logger.info(
                `New speaker: ${speakerInfo.name} (id: ${speakerInfo.id})`
              );
            }

            // For other JSON messages, log as usual without speaker tracking
          } else {
            logger.info(`Message from MeetingBaas: ${inspectMessage(message)}`);
          }
        } catch {
          // Likely audio data, send to transcription provider
          const audioBuffer = Buffer.isBuffer(message) ? message : Buffer.from(message);
          const now = Date.now();

          // Latency tracking
          if (!this.firstAudioTime) {
            this.firstAudioTime = now;
            const timeToFirstAudio = this.connectionTime ? now - this.connectionTime : 0;
            logger.info(`🎯 First audio chunk received! Time since connection: ${timeToFirstAudio}ms, chunk size: ${audioBuffer.length} bytes`);
          }
          if (this.lastChunkTime) {
            const gap = now - this.lastChunkTime;
            this.interChunkDelays.push(gap);
            if (gap > 100) {
              this.gapCount++;
              logger.warn(`⚠️ Audio gap detected: ${gap}ms between chunks (causes crackling)`);
            }
          }
          this.lastChunkTime = now;
          this.chunkCount++;
          this.totalBytes += audioBuffer.length;
          this.chunkSizes.push(audioBuffer.length);

          // Audio quality analysis (16-bit PCM signed LE)
          this.analyzeAudioChunk(audioBuffer);

          getProcessLogger()?.debug(
            `Received audio chunk from MeetingBaas`,
            "Proxy",
            { size: audioBuffer.length, isGladiaActive: this.isGladiaSessionActive }
          );

          if (this.transcriptionEnabled) {
            if (this.isGladiaSessionActive && this.gladiaClient) {
              getProcessLogger()?.debug(`Sending audio chunk to Gladia`, "Proxy", { size: audioBuffer.length });
              this.gladiaClient.sendAudioChunk(audioBuffer).catch((error) => {
                logger.error("❌ Error sending audio chunk to transcription:", error);
                getProcessLogger()?.error(`Failed to send audio chunk to Gladia`, "Proxy", { error: error.message });
              });
            } else {
              logger.warn(`⚠️ Transcription session not active yet, dropping audio chunk`);
              getProcessLogger()?.warn(`Gladia session not active, dropping audio chunk`, "Proxy");
            }
          }

          // Update audio visualizer (no buffering, so buffer pressure always 0)
          this.audioVisualizer.update(audioBuffer, this.lastSpeaker || undefined, 0);

          // Play audio directly through speakers if enabled (no buffering)
          this.playAudio(audioBuffer);

          // Store audio buffer if recording is enabled
          if (proxyConfig.recording.enabled) {
            this.audioBuffers.push(Buffer.from(message));
          }
        }
      } else {
        // For non-binary messages, log as usual
        logger.info(`Message from MeetingBaas: ${inspectMessage(message)}`);
      }

      // Forward MeetingBaas messages to bot client
      if (this.botClient && this.botClient.readyState === WebSocket.OPEN) {
        this.botClient.send(message.toString());
      }
    });

    ws.on("close", async () => {
      logger.info("MeetingBaas client disconnected");
      this.meetingBaasClients.delete(ws);

      // Save audio and end Gladia session if last client disconnects
      if (this.meetingBaasClients.size === 0) {
        // Clear and reset the visualizer
        this.audioVisualizer.reset();

        await this.saveAudioToFile();

        if (this.isGladiaSessionActive && this.gladiaClient) {
          this.gladiaClient.endSession();
          this.isGladiaSessionActive = false;
        }

        // Reset transcription state for next connection
        this.transcriptionInitialized = false;
        // Only wait for recording status in remote mode
        this.waitingForRecordingStatus = this.mode !== "Local";

        // Show disconnection message
        logger.info("All clients disconnected. Waiting for new connections...");
      }
    });

    ws.on("error", (error) => {
      logger.error("MeetingBaas client error:", error);
      this.audioVisualizer?.showError(`MeetingBaas client error: ${error.message || error}`);
    });
  }

  /**
   * Initialize the transcription session
   * Called either immediately (local mode) or when bot.status_change indicates in_call_not_recording
   */
  private initializeTranscriptionSession(): void {
    if (!this.transcriptionEnabled || !this.gladiaClient) {
      logger.info("Transcription disabled - skipping session initialization");
      this.audioVisualizer.addLog("Echo mode - no transcription", "info");
      return;
    }

    if (this.transcriptionInitialized || this.isGladiaSessionActive) {
      logger.info("Transcription session already initialized, skipping");
      return;
    }

    this.transcriptionInitialized = true;
    logger.info("🔄 Initializing transcription session...");
    this.audioVisualizer.addLog("Starting transcription...", "info");

    this.gladiaClient!.initSession().then((success) => {
      this.isGladiaSessionActive = success;
      if (success) {
        logger.info("✅ Transcription session ready and active!");
        this.audioVisualizer.addLog("Transcription active!", "info");
      } else {
        // Get error message and truncate to 128 chars
        const rawError = this.gladiaClient!.getLastError();
        const errorMsg = rawError ? rawError.substring(0, 128) : "Unknown error";
        const displayMsg = `Failed with message: ${errorMsg}`;

        logger.error("❌ Failed to initialize transcription session:", displayMsg);

        // Print to stderr so it's visible even if TUI fails
        console.error("\n");
        console.error("╔════════════════════════════════════════════════════════════════╗");
        console.error("║                    ⚠️  CRITICAL ERROR  ⚠️                     ║");
        console.error("╠════════════════════════════════════════════════════════════════╣");
        console.error("║                                                                ║");
        console.error(`║  ${displayMsg.substring(0, 62).padEnd(62)}║`);
        console.error("║                                                                ║");
        console.error("║       Bot will exit the meeting in 3 seconds...                ║");
        console.error("║                                                                ║");
        console.error("╚════════════════════════════════════════════════════════════════╝");
        console.error("\n");

        // Show critical error in TUI (this is fatal - bot will exit)
        this.audioVisualizer?.showCriticalError(displayMsg);

        // Gracefully shutdown
        this.handleTranscriptionError();
      }
    }).catch((error) => {
      // Get error message and truncate to 128 chars
      const rawError = error.message || String(error);
      const errorMsg = rawError.substring(0, 128);
      const displayMsg = `Failed with message: ${errorMsg}`;

      logger.error("❌ Transcription initialization threw exception:", displayMsg);

      // Print to stderr so it's visible even if TUI fails
      console.error("\n");
      console.error("╔════════════════════════════════════════════════════════════════╗");
      console.error("║                    ⚠️  CRITICAL ERROR  ⚠️                     ║");
      console.error("╠════════════════════════════════════════════════════════════════╣");
      console.error("║                                                                ║");
      console.error(`║  ${displayMsg.substring(0, 62).padEnd(62)}║`);
      console.error("║                                                                ║");
      console.error("║       Bot will exit the meeting in 3 seconds...                ║");
      console.error("║                                                                ║");
      console.error("╚════════════════════════════════════════════════════════════════╝");
      console.error("\n");

      this.audioVisualizer?.showCriticalError(displayMsg);
      this.handleTranscriptionError();
    });
  }

  /**
   * Create WAV file header for the audio data
   */
  private createWavHeader(dataLength: number): Buffer {
    const sampleRate = proxyConfig.audioParams.sampleRate;
    const channels = proxyConfig.audioParams.channels;
    const bitsPerSample = 16;
    const byteRate = (sampleRate * channels * bitsPerSample) / 8;
    const blockAlign = (channels * bitsPerSample) / 8;

    const header = Buffer.alloc(44);

    // RIFF chunk descriptor
    header.write("RIFF", 0);
    header.writeUInt32LE(36 + dataLength, 4);
    header.write("WAVE", 8);

    // fmt sub-chunk
    header.write("fmt ", 12);
    header.writeUInt32LE(16, 16); // Sub-chunk size (16 for PCM)
    header.writeUInt16LE(1, 20); // Audio format (1 for PCM)
    header.writeUInt16LE(channels, 22);
    header.writeUInt32LE(sampleRate, 24);
    header.writeUInt32LE(byteRate, 28);
    header.writeUInt16LE(blockAlign, 32);
    header.writeUInt16LE(bitsPerSample, 34);

    // data sub-chunk
    header.write("data", 36);
    header.writeUInt32LE(dataLength, 40);

    return header;
  }

  /**
   * Save the concatenated audio to a WAV file
   */
  private async saveAudioToFile(): Promise<void> {
    if (!proxyConfig.recording.enabled || this.audioBuffers.length === 0) {
      return;
    }

    try {
      // Create output directory if it doesn't exist
      const outputDir = proxyConfig.recording.outputDir;
      if (!fs.existsSync(outputDir)) {
        fs.mkdirSync(outputDir, { recursive: true });
      }

      // Generate filename with timestamp
      const timestamp = this.recordingStartTime || Date.now();
      const filename = `recording_${new Date(timestamp).toISOString().replace(/[:.]/g, "-")}.wav`;
      const filepath = path.join(outputDir, filename);

      // Concatenate all audio buffers
      const audioData = Buffer.concat(this.audioBuffers);
      const wavHeader = this.createWavHeader(audioData.length);

      // Write WAV file
      const wavFile = Buffer.concat([wavHeader, audioData]);
      fs.writeFileSync(filepath, wavFile);

      // Store the recording info
      this.lastRecordingPath = filepath;
      this.lastRecordingSize = wavFile.length;

      logger.info(`Audio saved to: ${filepath} (${audioData.length} bytes)`);
    } catch (error) {
      logger.error("Error saving audio file:", error);
    }
  }

  /**
   * Handle transcription initialization error
   * Gracefully shuts down the app and exits the meeting
   */
  private handleTranscriptionError(): void {
    logger.error("🛑 Transcription service failed - initiating graceful shutdown");

    // Give time for error message to be visible in TUI
    setTimeout(async () => {
      // Close all client connections
      for (const client of this.meetingBaasClients) {
        try {
          client.close();
        } catch (error) {
          logger.error("Error closing client connection:", error);
        }
      }
      this.meetingBaasClients.clear();

      if (this.botClient) {
        try {
          this.botClient.close();
        } catch (error) {
          logger.error("Error closing bot connection:", error);
        }
        this.botClient = null;
      }

      // Shutdown proxy (saves audio, ends transcription session, etc.)
      await this.shutdown();

      // Exit process
      logger.info("Exiting due to transcription error");
      process.exit(1);
    }, 3000); // Wait 3 seconds so user can see the error
  }

  public async shutdown(): Promise<void> {
    // Log final stats
    if (this.chunkCount > 0) {
      const stats = this.getLatencyStats();
      logger.info(`📊 Final stats: ${stats.chunkCount} chunks, ${stats.totalMB} MB, avg gap: ${stats.avgInterChunkMs.toFixed(1)}ms, jitter: ${stats.jitterMs.toFixed(1)}ms`);
    }
    if (this.latencyLogInterval) clearInterval(this.latencyLogInterval);

    // Note: TUI cleanup is handled by index.ts (cleanupTUI) since it's a singleton

    // Stop audio playback
    if (this.speaker) {
      logger.info("Stopping audio playback...");
      try {
        this.speaker.end();
        this.speaker = null;
        this.isPlaybackReady = false;
      } catch (error) {
        logger.error("Error stopping speaker:", error);
      }
    }

    // Save audio recording if any data was captured
    await this.saveAudioToFile();

    // End the Gladia session if it's active
    if (this.isGladiaSessionActive && this.gladiaClient) {
      logger.info("Ending Gladia transcription session...");
      await this.gladiaClient.endSession();
      this.isGladiaSessionActive = false;
    }

    // Close all client connections
    this.meetingBaasClients.forEach((client) => {
      if (client.readyState === WebSocket.OPEN) {
        client.close();
      }
    });

    if (this.botClient && this.botClient.readyState === WebSocket.OPEN) {
      this.botClient.close();
    }

    // Close the WebSocket server
    return new Promise((resolve) => {
      this.server.close(() => {
        logger.info("WebSocket server closed");
        resolve();
      });
    });
  }

  /**
   * Get information about the last transcript session
   */
  public getLastTranscriptInfo(): { sessionDir: string; transcriptCount: number; duration: number } | null {
    return this.gladiaClient?.getLastTranscriptInfo() ?? null;
  }

  /**
   * Get information about the last audio recording
   */
  public getLastRecordingInfo(): { path: string; size: number } | null {
    if (this.lastRecordingPath) {
      return {
        path: this.lastRecordingPath,
        size: this.lastRecordingSize,
      };
    }
    return null;
  }
}

export { TranscriptionProxy };
