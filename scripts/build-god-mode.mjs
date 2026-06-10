import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const lines = fs.readFileSync(path.join(root, 'index-full-backup.html'), 'utf8').split('\n');

const slice = (start, end) => lines.slice(start - 1, end).join('\n');

const godCss = [
  slice(43, 43),
  slice(59, 333),
  `    body.god-mode-active { overflow: hidden; }`,
  `    #godApp.god-mode-fullscreen { position: fixed; inset: 0; z-index: 50; width: 100vw; height: 100vh; display: flex; flex-direction: column; background: #1e1e1e; overflow: hidden; }`,
  `    #godmodePanels.god-mode-workspace-shell { flex: 1; min-height: 0; overflow: hidden; display: flex; flex-direction: column; width: 100%; height: 100%; }`,
].join('\n');

const godPanelsHtml = slice(546, 629)
  .replace(/id="godmodePanels" class="hidden w-screen h-screen/, 'id="godmodePanels" class="hidden flex-1 min-h-0 w-full');

const scriptHeader = `    const PRODUCTION_API_BASE = 'https://ai-universe-core.onrender.com/';
    const REGISTERED_EMAIL = 'demoa2735@gmail.com';

    const STORAGE_KEYS = {
      apiBase: 'auc_api_base',
      adminKey: 'auc_admin_key',
      godMasterApiKey: 'auc_god_master_api_key',
    };

    const TRUNCATION_BADGE = '[TRUNCATED_BADGE_ACTIVE]';

    const state = {
      adminUnlocked: false,
      godWorkspace: {
        files: {},
        activeFile: null,
        treeEntries: [],
        localTreeEntries: [],
        localDirHandle: null,
        localDirName: '',
        localFileHandles: {},
        expandedDirs: new Set(['']),
        monacoReady: false,
        monacoDirty: false,
        editorMode: 'code',
        diffReview: {
          active: false,
          filePath: null,
          originalContent: '',
          proposedContent: '',
        },
        streamDiffBaseline: null,
        streamDiffBaselineCaptured: false,
        chatMessages: [],
      },
    };

    let godMonacoEditor = null;
    let godMonacoDiffEditor = null;
    let godMonacoSuppressDirty = false;
    let localDirectoryHandle = null;
    let monacoEditorInstance = null;
    let activeLocalFilePath = null;

    const el = {
      apiBase: document.getElementById('apiBase'),
      adminKey: document.getElementById('adminKey'),
      unlockGodModeBtn: document.getElementById('unlockGodModeBtn'),
      godmodeLocked: document.getElementById('godmodeLocked'),
      godmodePanels: document.getElementById('godmodePanels'),
      adminConsole: document.getElementById('adminConsole'),
      deepScrapeQuery: document.getElementById('deepScrapeQuery'),
      deepScrapeBtn: document.getElementById('deepScrapeBtn'),
      godDirectChatHistory: document.getElementById('godDirectChatHistory'),
      godDirectChatInput: document.getElementById('godDirectChatInput'),
      godDirectChatSendBtn: document.getElementById('godDirectChatSendBtn'),
      godFileExplorer: document.getElementById('godFileExplorer'),
      syncLocalFolderBtn: document.getElementById('syncLocalFolderBtn'),
      godLocalFolderBadge: document.getElementById('godLocalFolderBadge'),
      acceptGodMonacoBtn: document.getElementById('acceptGodMonacoBtn'),
      godDiffReviewBar: document.getElementById('godDiffReviewBar'),
      acceptGodDiffBtn: document.getElementById('acceptGodDiffBtn'),
      rejectGodDiffBtn: document.getElementById('rejectGodDiffBtn'),
      godMonacoEditorHost: document.getElementById('godMonacoEditorHost'),
      godMonacoDiffEditorHost: document.getElementById('godMonacoDiffEditor'),
      refreshGodFilesBtn: document.getElementById('refreshGodFilesBtn'),
      godMonacoTab: document.getElementById('godMonacoTab'),
      godEditorStack: document.querySelector('.god-editor-stack'),
      godCenterStack: document.querySelector('.god-center-stack'),
      godCursorGrid: document.querySelector('.god-cursor-grid'),
      godTerminal: document.getElementById('godTerminal'),
      toggleGodOpsBtn: document.getElementById('toggleGodOpsBtn'),
      godOpsPanel: document.getElementById('godOpsPanel'),
      toastHost: document.getElementById('toastHost'),
    };
`;

const utilsSlice = [
  slice(774, 784),
  `    function getClientEmail() {
      return REGISTERED_EMAIL;
    }`,
  slice(795, 798),
  slice(811, 836),
].join('\n\n');

const scriptBody = [
  utilsSlice,
  slice(877, 1960),
  slice(2027, 2050),
  slice(2325, 2588),
  slice(2637, 2644),
  slice(2668, 2715),
].join('\n\n');

const scriptFooter = `
    function isValidGodModeMasterKey(key) {
      const value = String(key || '').trim();
      return value.startsWith('sk_god_master_') || value.length >= 16;
    }

    function activateGodModeWorkspace() {
      document.getElementById('godAuthGate')?.classList.add('hidden');
      document.getElementById('godApp')?.classList.add('god-mode-fullscreen');
      el.godmodePanels?.classList.remove('hidden');
      el.godmodePanels?.classList.add('god-mode-workspace-shell');
      document.body.classList.add('god-mode-active');
      initGodMonacoEditor();
      refreshGodFileExplorer();
      layoutGodMonacoSurfaces();
    }

    function unlockGodMode() {
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
      appendGodTerminalLog('AI engine linked · POST /api/chat (God Mode session)', 'info');
      refreshGodFileExplorer();
    }

    function loadPersistedValues() {
      el.apiBase.value = getApiBase();
      el.adminKey.value = localStorage.getItem(STORAGE_KEYS.adminKey) || '';
      if (el.adminKey.value && isValidGodModeMasterKey(el.adminKey.value)) {
        state.adminUnlocked = true;
        activateGodModeWorkspace();
        appendGodTerminalLog(\`God Mode workspace restored · \${getApiBase()}\`, 'info');
      }
    }

    function bindEvents() {
      el.unlockGodModeBtn.addEventListener('click', unlockGodMode);
      el.adminKey.addEventListener('keydown', (event) => {
        if (event.key === 'Enter') {
          event.preventDefault();
          unlockGodMode();
        }
      });
      el.deepScrapeBtn.addEventListener('click', runDeepScrapeSearch);
      el.deepScrapeQuery.addEventListener('keydown', (event) => {
        if (event.key === 'Enter') {
          event.preventDefault();
          runDeepScrapeSearch();
        }
      });
      el.syncLocalFolderBtn.addEventListener('click', syncLocalGodWorkspaceFolder);
      el.acceptGodMonacoBtn.addEventListener('click', acceptGodMonacoChanges);
      el.acceptGodDiffBtn?.addEventListener('click', acceptGodDiffReview);
      el.rejectGodDiffBtn?.addEventListener('click', rejectGodDiffReview);
      document.addEventListener('keydown', (event) => {
        if (!state.godWorkspace.diffReview.active) return;
        if (event.key === 'Enter' && (event.ctrlKey || event.metaKey)) {
          event.preventDefault();
          acceptGodDiffReview();
        }
        if (event.key === 'Escape') {
          event.preventDefault();
          rejectGodDiffReview();
        }
      });
      el.refreshGodFilesBtn.addEventListener('click', refreshGodFileExplorer);
      el.toggleGodOpsBtn.addEventListener('click', () => {
        el.godOpsPanel.classList.toggle('hidden');
      });
      el.godDirectChatSendBtn.addEventListener('click', sendGodDirectChat);
      el.godDirectChatInput.addEventListener('keydown', (event) => {
        if (event.key === 'Enter') {
          event.preventDefault();
          sendGodDirectChat();
        }
      });
    }

    async function checkHealth() {
      try {
        const { response, payload } = await apiRequest('/health', { method: 'GET' });
        if (response.ok && state.adminUnlocked) {
          const redisHint = payload.redis ? \` · Redis \${payload.redis}\` : '';
          appendGodTerminalLog(\`Health OK · \${getApiBase()} · \${payload.status || 'online'}\${redisHint}\`, 'info');
        }
      } catch {
        if (state.adminUnlocked) appendGodTerminalLog('API health check failed.', 'warn');
      }
    }

    loadPersistedValues();
    bindEvents();
    checkHealth();
    if (!state.adminUnlocked) {
      showToast('Enter your God Mode master key to access the workspace.', 'info');
    }
`;

const html = `<!DOCTYPE html>
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
${godCss}
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
${godPanelsHtml}
  </div>
  <div id="toastHost" class="fixed bottom-4 right-4 z-50 space-y-2"></div>
  <script>
${scriptHeader}
${scriptBody}
${scriptFooter}
  </script>
</body>
</html>
`;

fs.writeFileSync(path.join(root, 'god-mode.html'), html, 'utf8');
console.log('god-mode.html rebuilt:', html.split('\n').length, 'lines');
