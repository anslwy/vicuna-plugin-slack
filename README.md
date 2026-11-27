# Vicuna Slack Bridge Plugin

Talk to any Vicuna Work session from Slack DMs or private channels over **Socket Mode** — with pairing, live progress edits, interactive Block Kit question/permission buttons, model switching, and image/file attachments. No public webhook URL or tunnel is required.

## Install in Vicuna (v0.0.69+)

1. Open **Plugins** in Vicuna and install **Slack** from **Explore** (or enter `anslwy/vicuna-plugin-slack` under **Installed**).
2. Create a Slack app using the manifest below.
3. Right-click any **Work** session → **Slack**.
4. Paste your **Bot User OAuth Token** (`xoxb-…`) and click **Save**.
5. When prompted, paste your **App-Level Token** (`xapp-…` with `connections:write` scope) and click **Connect**.
6. Send `!pair <code>` to the bot in a Slack DM or a private channel the bot was invited to.

> Multiple Vicuna Work sessions can share the same Slack app (paired to different private channels or DMs) — the bridge multiplexes them over a single Socket Mode connection. You can also use separate Slack apps per session.

## Create a Slack App (Manifest YAML)

1. Go to [Slack API Apps](https://api.slack.com/apps) → **Create New App** → **From a manifest**.
2. Pick your workspace and paste this YAML:

```yaml
display_information:
  name: Vicuna
  description: Bridge between Slack and Vicuna Work sessions
  background_color: "#4a154b"
features:
  app_home:
    home_tab_enabled: false
    messages_tab_enabled: true
    messages_tab_read_only_enabled: false
  bot_user:
    display_name: Vicuna
    always_online: true
oauth_config:
  scopes:
    bot:
      - chat:write
      - im:history
      - im:read
      - im:write
      - groups:history
      - groups:read
      - files:read
      - users:read
settings:
  event_subscriptions:
    bot_events:
      - message.im
      - message.groups
  interactivity:
    is_enabled: true
  org_deploy_enabled: false
  socket_mode_enabled: true
  token_rotation_enabled: false
```

3. Click **Create**, then **Install to Workspace** under **OAuth & Permissions** and copy the **Bot User OAuth Token** (`xoxb-…`).
4. Under **Basic Information** → **App-Level Tokens**, click **Generate Token and Scopes**, add the `connections:write` scope, and copy the **App-Level Token** (`xapp-…`).

## Commands (in Slack DM or private channel)

- `!pair <code>` — pair this DM or private channel to the Vicuna session
- `!status` — show whether the session is working/idle and its harness/model
- `!model` — pick harness and model with buttons (or `!model claude haiku`)
- `!now` (or `!send_now`) — interrupt the running turn and send queued messages now
- `!stop` — stop the running turn
- `!unpair` — disconnect this channel from the Vicuna session
- `!help` — list commands

## Build Locally

```bash
cargo build --release
```
