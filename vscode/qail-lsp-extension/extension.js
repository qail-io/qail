const fs = require("fs");
const path = require("path");
const vscode = require("vscode");
const { LanguageClient } = require("vscode-languageclient/node");

let client;
let outputChannel;

function withPlatformExecutable(binary) {
  if (process.platform !== "win32" || binary.endsWith(".exe")) {
    return binary;
  }
  return `${binary}.exe`;
}

function hasPathSeparator(value) {
  return value.includes("/") || value.includes("\\");
}

function getWorkspaceRoot() {
  return vscode.workspace.workspaceFolders?.[0]?.uri.fsPath;
}

function fileExists(filePath) {
  try {
    return fs.statSync(filePath).isFile();
  } catch {
    return false;
  }
}

function resolveConfiguredServerPath(serverPath, workspaceRoot) {
  const rawPath =
    typeof serverPath === "string" && serverPath.trim() !== ""
      ? serverPath.trim()
      : "qail-lsp";

  if (path.isAbsolute(rawPath)) {
    return withPlatformExecutable(rawPath);
  }
  if (hasPathSeparator(rawPath)) {
    if (workspaceRoot) {
      return withPlatformExecutable(path.join(workspaceRoot, rawPath));
    }
    return withPlatformExecutable(path.resolve(rawPath));
  }
  return rawPath;
}

function findOnPath(commandName) {
  const pathEnv = process.env.PATH || "";
  const suffix = process.platform === "win32" ? ".exe" : "";
  const command = commandName.endsWith(suffix) ? commandName : `${commandName}${suffix}`;

  for (const dir of pathEnv.split(path.delimiter)) {
    if (!dir) {
      continue;
    }
    const candidate = path.join(dir, command);
    if (fileExists(candidate)) {
      return candidate;
    }
  }

  return undefined;
}

function pickServerCommand(configuredServerPath, workspaceRoot) {
  const rawPath = typeof configuredServerPath === "string" ? configuredServerPath : "";
  const explicit = resolveConfiguredServerPath(configuredServerPath, workspaceRoot);
  const isDefault =
    rawPath === "" ||
    rawPath.trim() === "" ||
    rawPath.trim() === "qail-lsp";

  if (!isDefault) {
    return explicit;
  }

  const onPath = findOnPath("qail-lsp");
  if (onPath) {
    return onPath;
  }

  const fallbackCandidates = [];
  if (workspaceRoot) {
    fallbackCandidates.push(path.join(workspaceRoot, "target", "debug", withPlatformExecutable("qail-lsp")));
    fallbackCandidates.push(path.join(workspaceRoot, "target", "release", withPlatformExecutable("qail-lsp")));
  }

  return fallbackCandidates.find(fileExists) || explicit;
}

function buildServerOptions() {
  const cfg = vscode.workspace.getConfiguration("qailLsp");
  const configuredServerPath = cfg.get("serverPath", "qail-lsp");
  const serverPath = pickServerCommand(configuredServerPath, getWorkspaceRoot());
  const serverArgs = cfg.get("serverArgs", []);

  outputChannel?.appendLine(`[qail-lsp] Server command: ${serverPath}`);
  if (serverArgs.length > 0) {
    outputChannel?.appendLine(`[qail-lsp] Server args: ${JSON.stringify(serverArgs)}`);
  }

  return {
    command: serverPath,
    args: serverArgs,
    options: {}
  };
}

function buildClientOptions() {
  return {
    documentSelector: [
      { scheme: "file", language: "qail" },
      { scheme: "file", language: "rust" }
    ],
    synchronize: {
      configurationSection: "qailLsp",
      fileEvents: vscode.workspace.createFileSystemWatcher("**/*.{qail,rs}")
    }
  };
}

async function startClient(context) {
  if (client) {
    await client.stop();
    client = undefined;
  }

  client = new LanguageClient(
    "qailLsp",
    "QAIL Language Server",
    buildServerOptions(),
    buildClientOptions()
  );

  context.subscriptions.push(client.start());
}

function activate(context) {
  outputChannel = vscode.window.createOutputChannel("QAIL Language Server");
  context.subscriptions.push(outputChannel);

  context.subscriptions.push(
    vscode.commands.registerCommand("qailLsp.restart", async () => {
      try {
        await startClient(context);
        vscode.window.showInformationMessage("QAIL language server restarted.");
      } catch (err) {
        const message = err && err.message ? err.message : String(err);
        outputChannel?.appendLine(`[qail-lsp] Restart failed: ${message}`);
        vscode.window.showErrorMessage(`Failed to restart QAIL language server: ${message}`);
      }
    })
  );

  startClient(context).catch((err) => {
    const message = err && err.message ? err.message : String(err);
    outputChannel?.appendLine(`[qail-lsp] Startup failed: ${message}`);
    vscode.window.showErrorMessage(`Failed to start QAIL language server: ${message}`);
  });
}

async function deactivate() {
  if (client) {
    await client.stop();
    client = undefined;
  }
}

module.exports = {
  activate,
  deactivate
};
