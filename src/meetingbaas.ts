import { createBaasClient, type BaasClientV2Methods } from "@meeting-baas/sdk";
import { apiKeys, apiUrls } from "./config";
import { createLogger } from "./utils";
import { getProcessLogger } from "./processLogger";

const logger = createLogger("MeetingBaas");
const processLogger = getProcessLogger();

class MeetingBaasClient {
  private client: BaasClientV2Methods;
  private botId: string | null = null;

  constructor() {
    this.client = createBaasClient({
      api_key: apiKeys.meetingBaas,
      base_url: apiUrls.meetingBaas,
      api_version: "v2",
    });

    logger.info(`Initialized MeetingBaas SDK client with base URL: ${apiUrls.meetingBaas}`);
  }

  /**
   * Generate a unique deduplication key
   */
  private generateDeduplicationKey(botName: string): string {
    const timestamp = Date.now();
    const random = Math.random().toString(36).substring(2, 8);
    return `${botName}-${timestamp}-${random}`;
  }

  /**
   * Connect to a meeting via MeetingBaas
   * @param meetingUrl URL of the meeting to join
   * @param botName Name of the bot
   * @param streamingUrl WebSocket URL where MeetingBaas will stream audio (wss://)
   * @param webhookUrl HTTP/HTTPS URL for event notifications
   * @returns Promise that resolves when connected
   */
  async connect(
    meetingUrl: string,
    botName: string,
    streamingUrl?: string,
    webhookUrl?: string
  ): Promise<boolean> {
    try {
      logger.info(`Connecting to meeting: ${meetingUrl}`);
      processLogger?.info(
        `MeetingBaas connecting to meeting`,
        "MeetingBaas",
        { meetingUrl, botName, streamingUrl }
      );

      // Generate a unique deduplication key
      const deduplicationKey = this.generateDeduplicationKey(botName);
      logger.info(`Using deduplication key: ${deduplicationKey}`);

      // Convert HTTP/HTTPS URL to WebSocket URL if needed
      let wsUrl = streamingUrl;
      if (streamingUrl) {
        if (streamingUrl.startsWith("https://")) {
          wsUrl = streamingUrl.replace("https://", "wss://");
        } else if (streamingUrl.startsWith("http://")) {
          wsUrl = streamingUrl.replace("http://", "ws://");
        }
        logger.info(`Streaming audio to: ${wsUrl}`);
        processLogger?.info(
          `MeetingBaas will stream audio to`,
          "MeetingBaas",
          { originalUrl: streamingUrl, wsUrl }
        );
      }

      // Prepare join meeting configuration
      const joinConfig: any = {
        bot_name: botName,
        meeting_url: meetingUrl,
        reserved: false,
        deduplication_key: deduplicationKey, // Use unique deduplication key
        // Configure streaming to WebSocket
        streaming: {
          output: wsUrl, // WebSocket URL for streaming audio output
          audio_frequency: "16khz", // Audio frequency for streaming (matches Gladia requirements)
        },
      };

      // Add webhook URL - prioritize CLI argument over environment variable
      const finalWebhookUrl = webhookUrl || apiUrls.meetingBaasWebhook;
      if (finalWebhookUrl) {
        joinConfig.webhook_url = finalWebhookUrl;
        logger.info(`Using webhook URL for notifications: ${finalWebhookUrl}`);
      }

      // Join the meeting using the SDK
      processLogger?.info(
        `Calling MeetingBaas API createBot (v2)`,
        "MeetingBaas",
        { config: joinConfig }
      );

      const result = await this.client.createBot(joinConfig);

      if (result.success) {
        this.botId = result.data.bot_id;
        logger.info(`Bot created with ID: ${this.botId}`);
        logger.info(`API Response: ${JSON.stringify(result.data)}`);
        processLogger?.info(
          `MeetingBaas bot created successfully`,
          "MeetingBaas",
          { botId: this.botId, response: result.data }
        );
        return true;
      } else {
        logger.error("Failed to join meeting:", result.error);
        processLogger?.error(
          `MeetingBaas API error`,
          "MeetingBaas",
          { error: result.error }
        );
        return false;
      }
    } catch (error) {
      logger.error("Error connecting to meeting:", error);
      return false;
    }
  }

  public async disconnect(): Promise<void> {
    if (this.botId) {
      logger.info(`Requesting bot ${this.botId} to leave meeting...`);
      try {
        // SDK v6 v2 API uses leaveBot
        const result = await this.client.leaveBot({
          bot_id: this.botId,
        });

        if (result.success) {
          logger.info(`Bot ${this.botId} successfully left the meeting`);
        } else {
          logger.error(`Failed to leave meeting: ${JSON.stringify(result.error)}`);
        }
      } catch (error: any) {
        logger.error(`Exception leaving meeting: ${error.message || error}`);
      }

      this.botId = null;
    } else {
      logger.info("No bot ID to disconnect - bot may not have been created");
    }
  }

  public getBotId(): string | null {
    return this.botId;
  }
}

export { MeetingBaasClient };
