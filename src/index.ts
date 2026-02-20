import { MeetingBaasClient } from "./meetingbaas";
import { TranscriptionProxy } from "./proxy";
import { proxyConfig, webhookConfig, processLoggerConfig } from "./config";
import { createLogger } from "./utils";
import { WebhookHandler } from "./webhookHandler";
import { initProcessLogger, closeProcessLogger } from "./processLogger";
import { initTUI, cleanupTUI } from "./audioVisualizer";

// Initialize TUI FIRST - before any logging happens
// This ensures all console output is captured in the TUI from the start
initTUI("Starting", proxyConfig.port);

const logger = createLogger("Main");

// Initialize process logger
const processLogger = initProcessLogger(processLoggerConfig.outputDir, processLoggerConfig.enabled);
processLogger.info("Application starting", "Main");

// Keep references to all our clients for cleanup
let meetingBaasClient: MeetingBaasClient | null = null;
let proxy: TranscriptionProxy | null = null;
let webhookHandler: WebhookHandler | null = null;

type Mode = "local" | "remote";

// Track if shutdown is in progress to prevent double-execution
let isShuttingDown = false;

// Graceful shutdown handler
function setupGracefulShutdown() {
  process.on("SIGINT", async () => {
    // Prevent double-execution
    if (isShuttingDown) {
      logger.info("Shutdown already in progress, ignoring duplicate SIGINT");
      return;
    }
    isShuttingDown = true;

    logger.info("Shutting down gracefully...");

    // Stop webhook server
    if (webhookHandler) {
      logger.info("Stopping webhook server...");
      await webhookHandler.stop();
    }

    // Disconnect from MeetingBaas (remove the bot from the meeting)
    if (meetingBaasClient) {
      logger.info("Telling remote bot to leave the meeting...");
      await meetingBaasClient.disconnect();
    }

    // Close transcription services (via proxy)
    if (proxy) {
      logger.info("Closing transcription services...");
      await proxy.shutdown();
    }

    // Get data locations before closing loggers
    const processLogPath = processLogger?.getLogFilePath();
    const transcriptInfo = proxy?.getLastTranscriptInfo();
    const recordingInfo = proxy?.getLastRecordingInfo();

    // Close process logger
    closeProcessLogger();

    // Cleanup TUI (restores normal console output)
    cleanupTUI();

    // Display summary of stored data (now goes to normal console)
    displayDataStorageSummary(processLogPath, transcriptInfo, recordingInfo);

    process.exit(0);
  });
}

// Display a summary of where external data has been stored
function displayDataStorageSummary(
  processLogPath?: string,
  transcriptInfo?: { sessionDir: string; transcriptCount: number; duration: number } | null,
  recordingInfo?: { path: string; size: number } | null
) {
  console.log("\n");
  console.log("╔═══════════════════════════════════════════════════════════════════╗");
  console.log("║                    📁 DATA STORAGE SUMMARY                        ║");
  console.log("╚═══════════════════════════════════════════════════════════════════╝");
  console.log("");

  // Process logs
  if (processLogPath) {
    console.log("📝 Process Logs:");
    console.log(`   ${processLogPath}`);
    console.log("");
  }

  // Transcript sessions
  if (transcriptInfo) {
    console.log("💬 Transcription Session:");
    console.log(`   ${transcriptInfo.sessionDir}`);
    console.log(`   • Duration: ${transcriptInfo.duration.toFixed(2)}s`);
    console.log(`   • Transcripts: ${transcriptInfo.transcriptCount}`);
    console.log(`   • Files: transcript.json, transcript.txt, raw_logs.txt, session_info.txt`);
    console.log("");
  } else if (proxyConfig.transcriptLogging.enabled) {
    console.log("💬 Transcription Sessions:");
    console.log(`   ${proxyConfig.transcriptLogging.outputDir}/sessions/`);
    console.log(`   (No sessions in this run)`);
    console.log("");
  }

  // Audio recordings
  if (recordingInfo) {
    console.log("🎤 Audio Recording:");
    console.log(`   ${recordingInfo.path}`);
    console.log(`   • Size: ${(recordingInfo.size / 1024 / 1024).toFixed(2)} MB`);
    console.log("");
  } else if (proxyConfig.recording.enabled) {
    console.log("🎤 Audio Recordings:");
    console.log(`   ${proxyConfig.recording.outputDir}/`);
    console.log(`   (No recordings in this run)`);
    console.log("");
  }

  // Configuration status
  console.log("⚙️  Configuration:");
  console.log(`   • Transcript Logging: ${proxyConfig.transcriptLogging.enabled ? "ENABLED" : "DISABLED"}`);
  console.log(`   • Audio Recording: ${proxyConfig.recording.enabled ? "ENABLED" : "DISABLED"}`);
  console.log(`   • Transcription: ${proxyConfig.transcription.enabled ? "ENABLED" : "DISABLED"}`);
  console.log(`   • Audio Playback: ${proxyConfig.playback.enabled ? "ENABLED" : "DISABLED"}`);
  console.log("");

  console.log("═══════════════════════════════════════════════════════════════════");
  console.log("");
}

// Initialize webhook server if enabled
async function startWebhookServer() {
  if (!webhookConfig.enabled) {
    logger.info("Webhook server disabled (set ENABLE_WEBHOOKS=true to enable)");
    return;
  }

  try {
    webhookHandler = new WebhookHandler();

    // Register event handlers for different webhook events
    webhookHandler.on("transcription.completed", (event) => {
      logger.info(`Webhook: Transcription ${event.data?.id} completed`);
      // Add custom logic here (e.g., save to database, send notification)
    });

    webhookHandler.on("transcription.failed", (event) => {
      logger.error(`Webhook: Transcription ${event.data?.id} failed: ${event.data?.error}`);
      // Add custom error handling here
    });

    await webhookHandler.start();
    logger.info("✅ Webhook server started successfully");
  } catch (error: any) {
    logger.error(`Failed to start webhook server: ${error.message}`);
    logger.warn("Continuing without webhook server...");
  }
}

function showUsage() {
  console.log("\n╔════════════════════════════════════════════════════════════════╗");
  console.log("║           🎙️  Real-Time Meeting Transcription                ║");
  console.log("╚════════════════════════════════════════════════════════════════╝\n");
  console.log("Usage:");
  console.log("  npm run dev:local                                            # Proxy mode: wait for ANY WebSocket connection");
  console.log("  npm run dev:remote <meeting> [name] [streaming] [webhook]    # Managed mode: create MeetingBaas API bot\n");
  console.log("Proxy Mode (accept external audio sources):");
  console.log("  npm run dev:local");
  console.log("  → Starts WebSocket proxy on port 4040");
  console.log("  → Accepts connections from:");
  console.log("     • Local Docker bots");
  console.log("     • Remote streaming servers");
  console.log("     • Any WebSocket client sending audio\n");
  console.log("Managed Mode (MeetingBaas API):");
  console.log("  npm run dev:remote https://meet.google.com/abc-defg-hij");
  console.log("  npm run dev:remote https://meet.google.com/abc-defg-hij \"My Bot\"");
  console.log("  npm run dev:remote https://meet.google.com/abc-defg-hij \"My Bot\" wss://example.ngrok.io");
  console.log("  npm run dev:remote https://meet.google.com/abc-defg-hij \"My Bot\" wss://example.ngrok.io https://example.ngrok.io/webhook");
  console.log("  → Creates bot via MeetingBaas API");
  console.log("  → Bot streams audio to [streaming] (defaults to ws://localhost:4040)");
  console.log("  → Bot sends notifications to [webhook] (optional, can also set MEETING_BAAS_WEBHOOK_URL env var)\n");
  process.exit(1);
}

async function runLocalMode() {
  logger.info("🔌 PROXY MODE: Waiting for WebSocket connections...");
  logger.info(`📡 Proxy listening on port ${proxyConfig.port}`);
  logger.info(`💡 Connect any audio source to: ws://localhost:${proxyConfig.port}`);
  logger.info(`   Examples:`);
  logger.info(`   • Local Docker bot: ./run_bot_streaming.sh ws://localhost:${proxyConfig.port}`);
  logger.info(`   • Remote server: redirect audio stream to ws://your-ip:${proxyConfig.port}`);
  logger.info(`   • Custom client: connect WebSocket and send 16-bit PCM audio`);

  // Create proxy only
  proxy = new TranscriptionProxy("Local");

  // Start webhook server if enabled
  await startWebhookServer();

  // Setup graceful shutdown
  setupGracefulShutdown();

  logger.info("✅ Proxy ready - waiting for connections...");
}

async function runRemoteMode(
  meetingUrl: string,
  botName: string = "Transcription Bot",
  streamingUrl?: string,
  webhookUrl?: string
) {
  logger.info("☁️  REMOTE MODE: Creating bot via MeetingBaas API...");

  // Create proxy and MeetingBaas client
  proxy = new TranscriptionProxy("Remote");
  meetingBaasClient = new MeetingBaasClient();

  // Start webhook server if enabled
  await startWebhookServer();

  // Setup graceful shutdown
  setupGracefulShutdown();

  // Use custom streaming URL if provided, otherwise default to localhost
  const finalStreamingUrl = streamingUrl || `ws://localhost:${proxyConfig.port}`;
  logger.info(`Using streaming URL: ${finalStreamingUrl}`);

  // Connect the bot to the meeting
  const connected = await meetingBaasClient.connect(
    meetingUrl,
    botName,
    finalStreamingUrl,
    webhookUrl
  );

  if (!connected) {
    logger.error("Failed to create remote bot");
    process.exit(1);
  }

  logger.info("✅ Remote bot created and streaming to proxy");
}

async function main() {
  try {
    // Parse arguments
    const args = process.argv.slice(2);

    // Check for mode flag
    let mode: Mode = "remote"; // Default to remote for backwards compatibility
    let argIndex = 0;

    if (args[0] === "--local" || args[0] === "-l") {
      mode = "local";
      argIndex = 1;
    } else if (args[0] === "--remote" || args[0] === "-r") {
      mode = "remote";
      argIndex = 1;
    } else if (args[0] === "--help" || args[0] === "-h") {
      showUsage();
    }

    // Run appropriate mode
    if (mode === "local") {
      await runLocalMode();
    } else {
      // Remote mode requires meeting URL
      const meetingUrl = args[argIndex];
      const botName = args[argIndex + 1] || "Transcription Bot";
      const streamingUrl = args[argIndex + 2]; // Optional WebSocket streaming URL (wss://)
      let webhookUrl = args[argIndex + 3]; // Optional webhook URL for notifications (https://)

      if (!meetingUrl || meetingUrl.startsWith("-")) {
        logger.error("Remote mode requires a meeting URL");
        showUsage();
      }

      // Auto-derive webhook URL from streaming URL if not provided
      if (!webhookUrl && streamingUrl) {
        webhookUrl = streamingUrl
          .replace("wss://", "https://")
          .replace("ws://", "http://")
          + "/webhooks/meetingbaas";
        logger.info(`Auto-derived webhook URL: ${webhookUrl}`);
      }

      await runRemoteMode(meetingUrl, botName, streamingUrl, webhookUrl);
    }
  } catch (error) {
    logger.error("Error initializing system:", error);
    process.exit(1);
  }
}

main();
