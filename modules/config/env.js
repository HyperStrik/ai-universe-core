function stripQuotes(value) {
  if (!value) return '';
  return String(value).replace(/^['"]|['"]$/g, '');
}

function getEnv() {
  return {
    port: Number(process.env.PORT) || 5000,
    databaseUrl: process.env.DATABASE_URL,
    masterAdminKey: stripQuotes(process.env.MASTER_ADMIN_KEY),
    runpodAiUrl: stripQuotes(process.env.RUNPOD_AI_URL),
    runpodApiKey: stripQuotes(process.env.RUNPOD_API_KEY),
    ollamaApiUrl: stripQuotes(process.env.OLLAMA_API_URL) || 'http://localhost:11434',
    ollamaChatUrl:
      stripQuotes(process.env.OLLAMA_CHAT_URL) ||
      'http://localhost:11434/v1/chat/completions',
    ollamaModel: stripQuotes(process.env.OLLAMA_MODEL) || 'dolphin-llama3',
  };
}

module.exports = { getEnv, stripQuotes };
