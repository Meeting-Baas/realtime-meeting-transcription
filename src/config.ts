import * as dotenv from "dotenv";
import { resolve } from "path";

// Load environment variables from .env file
dotenv.config({ path: resolve(__dirname, "../.env") });

// Bot configuration
export const botConfig = {
  host: process.env.BOT_HOST || "0.0.0.0",
  port: parseInt(process.env.BOT_PORT || "8766"),
  audioParams: {
    sampleRate: 16000,
    channels: 1,
  },
};

// Proxy configuration
export const proxyConfig = {
  host: process.env.PROXY_HOST || "0.0.0.0",
  port: parseInt(process.env.PROXY_PORT || "4040"),
  botUrl: process.env.BOT_URL || "ws://localhost:8766",
  audioParams: {
    sampleRate: 16000,
    channels: 1,
  },
  audioConfig: {
    encoding: (process.env.AUDIO_ENCODING as "linear16" | "pcm_s16le" | "mulaw" | "alaw") || "linear16",
    sampleRate: parseInt(process.env.AUDIO_SAMPLE_RATE || "16000"),
    language: process.env.AUDIO_LANGUAGE || "en",
    channels: parseInt(process.env.AUDIO_CHANNELS || "1"),
  },
  recording: {
    enabled: process.env.ENABLE_AUDIO_RECORDING === "true",
    outputDir: process.env.AUDIO_OUTPUT_DIR || "./recordings",
  },
  playback: {
    enabled: process.env.ENABLE_AUDIO_PLAYBACK === "true",
  },
  transcription: {
    enabled: process.env.ENABLE_TRANSCRIPTION !== "false", // Enabled by default
  },
  transcriptLogging: {
    enabled: process.env.ENABLE_TRANSCRIPT_LOGGING !== "false", // Enabled by default
    outputDir: process.env.TRANSCRIPT_OUTPUT_DIR || "./transcripts",
  },
};

// API keys
export const apiKeys = {
  meetingBaas: process.env.MEETING_BAAS_API_KEY || "",
  gladia: process.env.GLADIA_API_KEY || "",
  deepgram: process.env.DEEPGRAM_API_KEY || "",
  assemblyai: process.env.ASSEMBLYAI_API_KEY || "",
  azureStt: process.env.AZURE_API_KEY || "",
  openaiWhisper: process.env.OPENAI_API_KEY || "",
  speechmatics: process.env.SPEECHMATICS_API_KEY || "",
};

// API URLs
export const apiUrls = {
  meetingBaas:
    process.env.MEETING_BAAS_API_URL || "https://api.meetingbaas.com",
  meetingBaasWebhook: process.env.MEETING_BAAS_WEBHOOK_URL || undefined,
};

// Valid transcription providers
const VALID_PROVIDERS = ["gladia", "deepgram", "assemblyai", "azure-stt", "openai-whisper", "speechmatics"] as const;
type ValidProvider = typeof VALID_PROVIDERS[number];

function getValidProvider(envValue: string | undefined, fallback: ValidProvider): ValidProvider {
  if (envValue && VALID_PROVIDERS.includes(envValue as ValidProvider)) {
    return envValue as ValidProvider;
  }
  return fallback;
}

// Valid selection strategies
const VALID_STRATEGIES = ["explicit", "default", "round-robin"] as const;
type ValidStrategy = typeof VALID_STRATEGIES[number];

function getValidStrategy(envValue: string | undefined, fallback: ValidStrategy): ValidStrategy {
  if (envValue && VALID_STRATEGIES.includes(envValue as ValidStrategy)) {
    return envValue as ValidStrategy;
  }
  return fallback;
}

// VoiceRouter configuration
export const voiceRouterConfig = {
  providers: {
    // Only include providers that have API keys configured
    ...(apiKeys.gladia && {
      gladia: { apiKey: apiKeys.gladia },  // v0.1.2+ works correctly
    }),
    ...(apiKeys.deepgram && {
      deepgram: { apiKey: apiKeys.deepgram },
    }),
    ...(apiKeys.assemblyai && {
      assemblyai: { apiKey: apiKeys.assemblyai },
    }),
    ...(apiKeys.azureStt &&
      process.env.AZURE_REGION && {
        "azure-stt": {
          apiKey: apiKeys.azureStt,
          region: process.env.AZURE_REGION,
        },
      }),
    ...(apiKeys.openaiWhisper && {
      "openai-whisper": { apiKey: apiKeys.openaiWhisper },
    }),
    ...(apiKeys.speechmatics && {
      speechmatics: { apiKey: apiKeys.speechmatics },
    }),
  },
  defaultProvider: getValidProvider(process.env.TRANSCRIPTION_PROVIDER, "gladia"),
  selectionStrategy: getValidStrategy(process.env.PROVIDER_STRATEGY, "default"),
};

// Webhook configuration
export const webhookConfig = {
  enabled: process.env.ENABLE_WEBHOOKS === "true",
  port: parseInt(process.env.WEBHOOK_PORT || "5050"),
  host: process.env.WEBHOOK_HOST || "0.0.0.0",
  path: process.env.WEBHOOK_PATH || "/webhooks/transcription",
  secret: process.env.WEBHOOK_SECRET,
};

// Warn if webhooks enabled without secret
if (webhookConfig.enabled && !webhookConfig.secret) {
  console.warn("⚠️  WARNING: Webhooks enabled without WEBHOOK_SECRET - requests will not be authenticated");
}

// Process logger configuration
export const processLoggerConfig = {
  enabled: process.env.ENABLE_PROCESS_LOGGING !== "false", // Enabled by default
  outputDir: process.env.PROCESS_LOG_DIR || "./logs",
};

if (!apiKeys.meetingBaas) {
  console.error("MEETING_BAAS_API_KEY is required");
  process.exit(1);
}

// Check if at least one transcription provider is configured (skip if transcription is disabled)
if (proxyConfig.transcription.enabled) {
  const hasProvider = Object.keys(voiceRouterConfig.providers).length > 0;
  if (!hasProvider) {
    console.error(
      "At least one transcription provider API key is required (GLADIA_API_KEY, DEEPGRAM_API_KEY, ASSEMBLYAI_API_KEY, etc.)"
    );
    process.exit(1);
  }
}
