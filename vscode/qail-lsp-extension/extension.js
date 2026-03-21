const path = require("path");
const vscode = require("vscode");
const { LanguageClient } = require("vscode-languageclient/node");

let client;

function resolveServerPath(serverPath) {
  if (path.isAbsolute(serverPath)) {
    return serverPath;
  }
  const workspace = vscode.workspace.workspaceFolders?.[0];
  if (!workspace) {
    return serverPath;
  }
  return path.join(workspace.uri.fsPath, serverPath);
}

function buildServerOptions() {
  const cfg = vscode.workspace.getConfiguration("qailLsp");
  const serverPath = resolveServerPath(cfg.get("serverPath", "qail-lsp"));
  const serverArgs = cfg.get("serverArgs", []);
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
  context.subscriptions.push(
    vscode.commands.registerCommand("qailLsp.restart", async () => {
      await startClient(context);
      vscode.window.showInformationMessage("QAIL language server restarted.");
    })
  );

  startClient(context).catch((err) => {
    vscode.window.showErrorMessage(`Failed to start QAIL language server: ${err.message}`);
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
