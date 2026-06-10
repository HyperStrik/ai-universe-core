import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const src = path.join(root, 'index-full-backup.html');
const lines = fs.readFileSync(src, 'utf8').split('\n');

const godCss = lines.slice(58, 333).join('\n'); // .god-cursor-grid through media query
const godInner = lines.slice(545, 629).join('\n'); // godmodePanels inner (skip auth header in section)

const godHtml = `<!DOCTYPE html>
<html lang="en" class="h-full">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>AI Universe Core — God Mode</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <style>
    html, body { height: 100%; margin: 0; }
    .chat-scroll { scrollbar-width: thin; scrollbar-color: #4b5563 #0f172a; }
    .chat-scroll::-webkit-scrollbar { width: 10px; }
    .console-scroll { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; }
    .god-scrape-glow { box-shadow: 0 0 24px rgba(59, 130, 246, 0.25), inset 0 0 0 1px rgba(96, 165, 250, 0.35); }
${godCss}
    body.god-mode-active { overflow: hidden; }
    #godApp.god-mode-fullscreen { position: fixed; inset: 0; z-index: 50; width: 100vw; height: 100vh; display: flex; flex-direction: column; background: #1e1e1e; overflow: hidden; }
    #godmodePanels.god-mode-workspace-shell { flex: 1; min-height: 0; overflow: hidden; display: flex; flex-direction: column; }
  </style>
  <script src="https://cdn.jsdelivr.net/npm/monaco-editor@0.45.0/min/vs/loader.js"></script>
</head>
<body class="h-full bg-[#1e1e1e] text-white antialiased">
  <div id="godApp" class="h-full flex flex-col">
    <div id="godAuthGate" class="min-h-screen flex items-center justify-center p-6 bg-gradient-to-br from-[#0b0f17] via-[#111827] to-[#1e1e1e]">
      <div class="w-full max-w-md rounded-2xl border border-[#3c3c3c] bg-[#252526] p-6 shadow-2xl space-y-4">
        <p class="text-xs uppercase tracking-[0.2em] text-[#4ec9b0] font-semibold text-center">Private Operator Console</p>
        <h1 class="text-xl font-bold text-center">God Mode Workspace</h1>
        <p class="text-sm text-[#858585] text-center">Requires <code class="text-[#9cdcfe]">sk_god_master_...</code> or authorized master key.</p>
        <label class="block text-xs text-[#858585] uppercase tracking-wide">API Base URL</label>
        <input id="apiBase" type="url" class="w-full rounded-lg bg-[#1e1e1e] border border-[#3c3c3c] px-3 py-2 text-sm text-white" />
        <label class="block text-xs text-[#858585] uppercase tracking-wide">God Mode Master Key</label>
        <input id="adminKey" type="password" placeholder="sk_god_master_..." class="w-full rounded-lg bg-[#1e1e1e] border border-[#3c3c3c] px-3 py-2 text-sm text-white" />
        <button id="unlockGodModeBtn" type="button" class="w-full rounded-lg bg-[#007acc] hover:bg-[#1a8ad4] text-white font-semibold py-2.5 text-sm">Unlock God Mode</button>
        <p class="text-[11px] text-center"><a href="index.html" class="text-[#3794ff] hover:underline">← Public dashboard</a></p>
      </div>
    </div>
    <div id="godmodeLocked" class="hidden"></div>
${godInner.replace(/^        /gm, '    ')}
  </div>
  <div id="toastHost" class="fixed bottom-4 right-4 z-50 space-y-2"></div>
  <script>
GOD_SCRIPT_PLACEHOLDER
  </script>
</body>
</html>`;

let script = lines.slice(636).join('\n').replace(/^  <script>\n/, '').replace(/\n  <\/script>\n<\/body>\n<\/html>\s*$/, '');

function stripBetween(srcText, startMarker, endMarker) {
  const start = srcText.indexOf(startMarker);
  const end = srcText.indexOf(endMarker, start);
  if (start === -1 || end === -1) return srcText;
  return srcText.slice(0, start) + srcText.slice(end);
}

// --- God script ---
let godScript = script;

godScript = godScript.replace(/const SCREEN_META = \{[\s\S]*?\};\n\n/, '');
godScript = godScript.replace(/activeScreen: 'chat'/, "activeScreen: 'godmode'");
godScript = godScript.replace(
  /      shareCountToday: 0,\n      pricingReason: '',\n      sessionMsRemaining: null,\n      otpAutoSendAttempted: false,\n      developerApi: \{[\s\S]*?\},\n      commercialChat: \{[\s\S]*?\},\n/,
  ''
);
godScript = godScript.replace(/      godmode: \{[\s\S]*?\},\n/, '');

const godElKeys = [
  'userEmail', 'otpVerification', 'verifyOtpBtn', 'resendOtpBtn', 'deviceFingerprint', 'saveIdentityBtn',
  'developerApiCard', 'developerTokenBalanceBadge', 'developerCreditsHint', 'developerApiKeyInput',
  'copyDeveloperApiKeyBtn', 'createDeveloperApiKeyBtn', 'screenTitle', 'screenSubtitle', 'statusPill',
  'trialPaywallAlert', 'trialPaywallTime', 'trialUpgradeBtn', 'chatTimeline', 'chatInput', 'sendChatBtn',
  'whatsappShareBtn', 'shareCountHint', 'creditHint', 'pricingAlert', 'pricingAlertText', 'screenGodmode',
];
for (const key of godElKeys) {
  godScript = godScript.replace(new RegExp(`\\s+${key}: [^\\n]+\\n`, 'g'), '');
}

godScript = godScript.replace(
  /    function setActiveScreen\([\s\S]*?    \}\n\n    let godMonacoLayoutRaf/,
  `    function activateGodModeWorkspace() {
      document.getElementById('godAuthGate')?.classList.add('hidden');
      document.getElementById('godApp')?.classList.add('god-mode-fullscreen');
      el.godmodePanels?.classList.remove('hidden');
      document.body.classList.add('god-mode-active');
      initGodMonacoEditor();
      refreshGodFileExplorer();
      layoutGodMonacoSurfaces();
    }

    function isValidGodModeMasterKey(key) {
      const value = String(key || '').trim();
      return value.startsWith('sk_god_master_') || value.length >= 16;
    }

    let godMonacoLayoutRaf`
);

godScript = stripBetween(godScript, '    function createChatBubble(', '    function appendGodDirectChatEntry(');
godScript = stripBetween(godScript, '    async function trackWhatsappShare(', '    function logAdminConsole(');
godScript = stripBetween(godScript, '    function getCommercialChatBearerToken(', '    function buildGodWorkspaceContextSystemMessage(');
godScript = stripBetween(
  godScript,
  '    function persistCommercialApiKey(',
  '    function unlockGodMode('
);

godScript = godScript.replace(
  /function unlockGodMode\(\) \{[\s\S]*?refreshGodFileExplorer\(\);\n    \}/,
  `function unlockGodMode() {
      const key = el.adminKey.value.trim();
      if (!isValidGodModeMasterKey(key)) {
        showToast('Enter a valid God Mode master key (sk_god_master_...).', 'warn');
        return;
      }
      localStorage.setItem(STORAGE_KEYS.adminKey, key);
      state.adminUnlocked = true;
      showToast('God Mode session unlocked.', 'success');
      logAdminConsole('SESSION', { status: 'god_mode_unlocked' });
      activateGodModeWorkspace();
      appendGodTerminalLog(\`God Mode workspace online · \${getApiBase()}\`, 'info');
      refreshGodFileExplorer();
    }`
);

godScript = godScript.replace(
  /function loadPersistedValues\(\) \{[\s\S]*?\n    \}\n\n    function bindEvents/,
  `function loadPersistedValues() {
      el.apiBase.value = getApiBase();
      el.adminKey.value = localStorage.getItem(STORAGE_KEYS.adminKey) || '';
      if (el.adminKey.value && isValidGodModeMasterKey(el.adminKey.value)) {
        state.adminUnlocked = true;
        activateGodModeWorkspace();
        appendGodTerminalLog(\`God Mode workspace restored · \${getApiBase()}\`, 'info');
        refreshGodFileExplorer();
      }
    }

    function bindEvents`
);

godScript = godScript.replace(/document\.querySelectorAll\('\.nav-btn'\)[\s\S]*?\}\);\n\n/, '');
godScript = godScript.replace(/el\.saveIdentityBtn[\s\S]*?el\.resendOtpBtn[\s\S]*?\}\);\n/, '');
godScript = godScript.replace(/el\.sendChatBtn[\s\S]*?trackWhatsappShare\);\n/, '');
godScript = godScript.replace(/document\.querySelectorAll\('\.pricing-buy'\)[\s\S]*?\}\);\n/, '');
godScript = godScript.replace(/async function loadSessionTrialState\([\s\S]*?\n    \}\n\n    loadPersistedValues/, 'loadPersistedValues');
godScript = godScript.replace(
  /appendChatMessage\('assistant', 'Welcome[\s\S]*?\['Ready'\]\);/,
  "appendGodTerminalLog('God Mode console ready. Unlock with your master key.', 'info');"
);
godScript = godScript.replace(
  /async function checkHealth\(\) \{[\s\S]*?\n    \}/,
  `async function checkHealth() {
      try {
        const { response, payload } = await apiRequest('/health', { method: 'GET' });
        if (response.ok && state.adminUnlocked) {
          const redisHint = payload.redis ? \` · Redis \${payload.redis}\` : '';
          appendGodTerminalLog(\`Health OK · \${getApiBase()} · \${payload.status || 'online'}\${redisHint}\`, 'info');
        }
      } catch {
        if (state.adminUnlocked) appendGodTerminalLog('API health check failed.', 'warn');
      }
    }`
);

fs.writeFileSync(path.join(root, 'god-mode.html'), godHtml.replace('GOD_SCRIPT_PLACEHOLDER', godScript), 'utf8');

// --- Public index ---
const publicHead = [
  ...lines.slice(0, 42),
  ...lines.slice(43, 58),
  '  </style>',
  '</head>',
  ...lines.slice(336, 352),
  '        <a href="god-mode.html" class="block w-full text-left px-4 py-3 rounded-xl hover:bg-slate-800 border border-transparent text-slate-200 text-sm">God Mode Console ↗</a>',
  ...lines.slice(353, 531),
  ...lines.slice(630, 635),
].join('\n');

let publicScript = script;

publicScript = publicScript.replace(/      godmode: \{[\s\S]*?\},\n/, '');
publicScript = publicScript.replace(/      adminUnlocked: false,\n/, '');
publicScript = publicScript.replace(/      commercialChat: \{[\s\S]*?\},\n/, '');
publicScript = publicScript.replace(/      godWorkspace: \{[\s\S]*?\},\n/, '');
publicScript = publicScript.replace(/    let godMonacoEditor[\s\S]*?let activeLocalFilePath = null;\n\n/, '');

const publicElKeys = [
  'deepScrapeQuery', 'deepScrapeBtn', 'adminKey', 'unlockGodModeBtn', 'godmodeLocked', 'godmodePanels',
  'adminConsole', 'godDirectChatHistory', 'godDirectChatInput', 'godDirectChatSendBtn', 'godFileExplorer',
  'syncLocalFolderBtn', 'godLocalFolderBadge', 'acceptGodMonacoBtn', 'godDiffReviewBar', 'acceptGodDiffBtn',
  'rejectGodDiffBtn', 'godMonacoEditorHost', 'godMonacoDiffEditorHost', 'screenGodmode', 'refreshGodFilesBtn',
  'godMonacoTab', 'godEditorStack', 'godCenterStack', 'godCursorGrid', 'godTerminal', 'toggleGodOpsBtn', 'godOpsPanel',
];
for (const key of publicElKeys) {
  publicScript = publicScript.replace(new RegExp(`\\s+${key}: [^\\n]+\\n`, 'g'), '');
}

publicScript = publicScript.replace(
  /      const godFullscreen[\s\S]*?layoutGodMonacoSurfaces\(\);\n      \}\n    \}/,
  '    }'
);

publicScript = stripBetween(publicScript, '    let godMonacoLayoutRaf = 0;', '    function createChatBubble(');
publicScript = stripBetween(publicScript, '    function appendGodDirectChatEntry(', '    async function runDeepScrapeSearch(');
publicScript = stripBetween(publicScript, '    async function runDeepScrapeSearch(', '    async function trackWhatsappShare(');
publicScript = stripBetween(publicScript, '    function buildGodWorkspaceContextSystemMessage(', '    function persistCommercialApiKey(');

publicScript = publicScript.replace(
  /el\.deepScrapeBtn[\s\S]*?sendGodDirectChat\(\);\n        \}\n      \}\);\n\n/,
  ''
);

publicScript = publicScript.replace(
  /function loadPersistedValues\(\) \{[\s\S]*?\n    \}/,
  `function loadPersistedValues() {
      localStorage.removeItem(STORAGE_KEYS.apiBase);
      el.apiBase.value = getApiBase();
      el.userEmail.value = localStorage.getItem(STORAGE_KEYS.email) || '';
      el.deviceFingerprint.value = ensureDeviceFingerprint();
      const storedCommercialKey = localStorage.getItem(STORAGE_KEYS.commercialApiKey) || '';
      if (storedCommercialKey.startsWith('sk_client_') && el.developerApiKeyInput) {
        el.developerApiKeyInput.value = storedCommercialKey;
        el.developerApiKeyInput.type = 'password';
        state.developerApi.fullKeyVisible = true;
      }
    }`
);

publicScript = publicScript.replace(
  /async function loadSessionTrialState\(\) \{[\s\S]*?const adminKey[\s\S]*?if \(godModeActive\) \{[\s\S]*?\}\n\n      const email/,
  'async function loadSessionTrialState() {\n      const email'
);

publicScript = publicScript.replace(
  /async function checkHealth\(\) \{[\s\S]*?if \(state\.adminUnlocked\) \{[\s\S]*?\}\n        \} else/,
  `async function checkHealth() {
      try {
        const { response, payload } = await apiRequest('/health', { method: 'GET' });
        if (response.ok) {
          const redisHint = payload.redis ? \` · Redis \${payload.redis}\` : '';
          el.statusPill.textContent = \`System \${payload.status || 'online'}\${redisHint}\`;
          el.statusPill.className = 'text-xs px-3 py-1 rounded-full bg-emerald-500/20 text-emerald-300 border border-emerald-500/30';
        } else`
);

publicScript = publicScript.replace(/    function getAdminHeaders\(\) \{[\s\S]*?\}\n\n/, '');

fs.writeFileSync(
  path.join(root, 'index.html'),
  `${publicHead}\n  <script>\n${publicScript}\n  </script>\n</body>\n</html>\n`,
  'utf8'
);

console.log('Done. index lines:', publicHead.split('\n').length + publicScript.split('\n').length);
console.log('god-mode lines:', fs.readFileSync(path.join(root, 'god-mode.html'), 'utf8').split('\n').length);
