require 'base64'
require 'json'

account = Account.create!(name: 'Ticket01 synthetic account')
user = User.create!(
  name: 'Ticket01 synthetic operator',
  email: 'ticket01@example.invalid',
  password: ENV.fetch('TICKET01_USER_PASSWORD'),
  password_confirmation: ENV.fetch('TICKET01_USER_PASSWORD'),
  confirmed_at: Time.current
)
AccountUser.create!(account: account, user: user, role: :administrator)
access_token = AccessToken.create!(owner: user)

channel = Channel::Api.create!(account: account, webhook_url: '')
inbox = Inbox.create!(
  account: account,
  channel: channel,
  name: 'Ticket01 synthetic inbox'
)
contact = Contact.create!(
  account: account,
  name: 'Ticket01 synthetic contact',
  phone_number: '+15550000001'
)
contact_inbox = ContactInbox.create!(
  contact: contact,
  inbox: inbox,
  source_id: 'ticket01-synthetic-contact-source'
)
conversation = Conversation.create!(
  account: account,
  inbox: inbox,
  contact: contact,
  contact_inbox: contact_inbox,
  status: :open
)

# Bypass callbacks only for old synthetic pagination filler. Provider-note
# creation and every boundary assertion still use the real HTTP endpoint.
old_time = 2.days.ago
filler = 205.times.map do |ordinal|
  {
    account_id: account.id,
    inbox_id: inbox.id,
    conversation_id: conversation.id,
    message_type: Message.message_types.fetch('activity'),
    private: false,
    content: "synthetic pagination filler #{ordinal}",
    content_type: Message.content_types.fetch('text'),
    status: Message.statuses.fetch('sent'),
    created_at: old_time + ordinal.seconds,
    updated_at: old_time + ordinal.seconds
  }
end
Message.insert_all!(filler)

fixture = {
  account_id: account.id,
  api_token: access_token.token,
  inbox_id: inbox.id,
  contact_id: contact.id,
  conversation_display_id: conversation.display_id
}
puts "TICKET01_SEED=#{Base64.strict_encode64(fixture.to_json)}"
