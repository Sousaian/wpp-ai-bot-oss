#!/usr/bin/env python3
import argparse
import hashlib
import json
from pathlib import Path


EVOLUTION_DIGEST = (
    "sha256:966625532d9076a2381e973a271307d107e6f070450de3abeeea8bd18be07252"
)
CHATWOOT_DIGEST = (
    "sha256:18f280a63070073c7588164eed31303cc788123d899733e54aeec4c39d1d8dd9"
)


def source_from_map(path: Path, suffix: str) -> str:
    source_map = json.loads(path.read_text())
    matches = [
        content
        for source, content in zip(
            source_map["sources"], source_map["sourcesContent"], strict=True
        )
        if source.endswith(suffix)
    ]
    assert len(matches) == 1, (path, suffix, len(matches))
    return matches[0]


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def verify_evolution(root: Path) -> dict[str, object]:
    package = json.loads((root / "evolution/package.json").read_text())
    assert package["name"] == "evolution-api"
    assert package["version"] == "2.3.7"

    route_map = root / "evolution/sendMessage.router.js.map"
    controller_map = root / "evolution/sendMessage.controller.js.map"
    chat_router = source_from_map(route_map, "src/api/routes/chat.router.ts")
    chat_schema = source_from_map(route_map, "src/validate/chat.schema.ts")
    chat_dto = source_from_map(route_map, "src/api/dto/chat.dto.ts")
    chat_controller = source_from_map(
        controller_map, "src/api/controllers/chat.controller.ts"
    )
    whatsapp = source_from_map(
        controller_map,
        "src/api/integrations/channel/whatsapp/whatsapp.baileys.service.ts",
    )

    assert ".post(this.routerPath('sendPresence')" in chat_router
    assert "return res.status(HttpStatus.CREATED).json(response);" in chat_router
    assert "schema: presenceSchema" in chat_router
    assert "execute: (instance, data) => chatController.sendPresence(instance, data)" in chat_router
    presence_schema_start = chat_schema.index("export const presenceSchema")
    presence_schema_end = chat_schema.index(
        "export const blockUserSchema", presence_schema_start
    )
    presence_schema = chat_schema[presence_schema_start:presence_schema_end]
    assert "number: { ...numberDefinition }" in presence_schema
    assert "delay: { type: 'number' }" in presence_schema
    assert "enum: ['unavailable', 'available', 'composing', 'recording', 'paused']" in presence_schema
    assert "required: ['number', 'presence', 'delay']" in presence_schema
    assert "presence: WAPresence;" in chat_dto
    assert "delay: number;" in chat_dto
    assert "return await this.waMonitor.waInstances[instanceName].sendPresence(data);" in chat_controller

    method_start = whatsapp.index("public async sendPresence(data: SendPresenceDto)")
    method_end = whatsapp.index("// Presence Controller", method_start)
    presence_method = whatsapp[method_start:method_end]
    assert "if (data?.delay && data?.delay > 20000)" in presence_method
    assert "await delay(20000);" in presence_method
    assert "await delay(data?.delay);" in presence_method
    assert "sendPresenceUpdate((data?.presence as WAPresence) ?? 'composing', sender)" in presence_method
    assert "sendPresenceUpdate('paused', sender)" in presence_method
    assert "return { presence: data.presence };" in presence_method
    assert "sendMessage(" not in presence_method
    assert "textMessage(" not in presence_method
    assert "minimum" not in presence_schema and "maximum" not in presence_schema

    return {
        "image_digest": EVOLUTION_DIGEST,
        "image_version": package["version"],
        "route": "POST /chat/sendPresence/:instanceName",
        "payload": ["number", "presence", "delay"],
        "delay_unit": "milliseconds",
        "response_status": 201,
        "response_blocks_through_delay_and_auto_pause": True,
        "provider_delay_bounds": None,
        "source_map_sha256": {
            route_map.name: sha256(route_map),
            controller_map.name: sha256(controller_map),
        },
    }


def verify_chatwoot(root: Path) -> dict[str, object]:
    package = json.loads((root / "chatwoot/package.json").read_text())
    assert package["name"] == "@chatwoot/chatwoot"
    assert package["version"] == "4.17.1"

    builder_path = root / "chatwoot/message_builder.rb"
    model_path = root / "chatwoot/message.rb"
    finder_path = root / "chatwoot/message_finder.rb"
    controller_path = root / "chatwoot/messages_controller.rb"
    view_path = root / "chatwoot/_message.json.jbuilder"
    send_job_path = root / "chatwoot/send_reply_job.rb"
    api_sender_path = root / "chatwoot/send_email_notification_service.rb"
    builder = builder_path.read_text()
    model = model_path.read_text()
    finder = finder_path.read_text()
    controller = controller_path.read_text()
    view = view_path.read_text()
    send_job = send_job_path.read_text()
    api_sender = api_sender_path.read_text()

    assert "@private = params[:private] || false" in builder
    assert "account_id: @conversation.account_id" in builder
    assert "inbox_id: @conversation.inbox_id" in builder
    assert "private: @private" in builder
    assert "source_id: @params[:source_id]" in builder
    assert "validates :content, length: { maximum: 150_000 }" in model
    assert "return false if private?" in model
    assert "'Channel::Api' => ::Messages::SendEmailNotificationService" in send_job
    assert "return unless should_send_email_notification?" in api_sender
    assert "return false unless message.email_notifiable_message?" in api_sender
    assert "@messages = message_finder.perform" in controller
    assert "Messages::MessageBuilder.new(user, @conversation, params)" in controller
    assert "messages.reorder('created_at asc').where('id > ?', after_id).limit(100)" in finder
    assert "messages.reorder('created_at desc').where('id < ?', before_id).limit(20).reverse" in finder
    assert "messages.reorder('created_at desc').limit(20).reverse" in finder
    assert "where.not('private = ? OR message_type = ?', true, 2)" in finder
    for field in ("id", "content", "inbox_id", "private", "source_id"):
        assert f"json.{field} message.{field}" in view
    assert "json.conversation_id message.conversation.display_id" in view

    schema = (root / "chatwoot/schema.rb").read_text()
    assert 't.index ["source_id"], name: "index_messages_on_source_id"' in schema
    assert 'name: "index_messages_on_source_id", unique: true' not in schema

    return {
        "image_digest": CHATWOOT_DIGEST,
        "image_version": package["version"],
        "route": "POST/GET /api/v1/accounts/:account_id/conversations/:conversation_id/messages",
        "private_flag": True,
        "content_limit_characters": 150_000,
        "after_page_size": 100,
        "latest_before_page_size": 20,
        "source_id_unique": False,
        "source_sha256": {
            builder_path.name: sha256(builder_path),
            model_path.name: sha256(model_path),
            finder_path.name: sha256(finder_path),
            controller_path.name: sha256(controller_path),
            view_path.name: sha256(view_path),
            send_job_path.name: sha256(send_job_path),
            api_sender_path.name: sha256(api_sender_path),
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("extract_root", type=Path)
    args = parser.parse_args()
    report = {
        "evidence": "PINNED_SOURCE_PROOF",
        "evolution": verify_evolution(args.extract_root),
        "chatwoot": verify_chatwoot(args.extract_root),
    }
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
