# frozen_string_literal: true

# Run with:
# docker compose exec -T chatwoot bundle exec rails runner - \
#   < chatwoot/test/phone_log_redaction_check.rb

filters = Rails.application.config.filter_parameters
upstream_password_filter_present = filters.any? do |filter|
  filter.to_s == 'password' || (filter.is_a?(Regexp) && filter.match?('password'))
end
raise 'upstream filter_parameters were replaced' unless upstream_password_filter_present

synthetic_phone = ['55', '11', '98765', '4321'].join
parameter_filter = ActiveSupport::ParameterFilter.new(filters)
filtered = parameter_filter.filter(
  'q' => synthetic_phone,
  'phone_number' => "+#{synthetic_phone}",
  'name' => synthetic_phone,
  'content' => 'synthetic private reply',
  'api_access_token' => 'synthetic-secret',
  'safe' => 'active'
)

raise 'q was not filtered' unless filtered['q'] == '[FILTERED]'
raise 'phone_number was not filtered' unless filtered['phone_number'] == '[FILTERED]'
raise 'phone-like name was not filtered' unless filtered['name'] == '[FILTERED]'
raise 'message content was not filtered' unless filtered['content'] == '[FILTERED]'
raise 'credential was not filtered' unless filtered['api_access_token'] == '[FILTERED]'
raise 'safe metadata was changed' unless filtered['safe'] == 'active'
raise 'Active Job arguments are loggable' unless Rails.application.config.active_job.log_arguments == false

env = Rack::MockRequest.env_for(
  "/api/v1/accounts/1/contacts/search?q=#{synthetic_phone}&include_contacts=true"
)
env['action_dispatch.parameter_filter'] = filters
filtered_path = ActionDispatch::Request.new(env).filtered_path

raise 'Started path leaked the phone' if filtered_path.include?(synthetic_phone)
unless filtered_path.start_with?('/api/v1/accounts/1/contacts/search?q=[FILTERED]') &&
       filtered_path.include?('include_contacts=true')
  raise 'Started path lost route shape or safe query metadata'
end

puts 'Chatwoot phone log redaction checks passed'
