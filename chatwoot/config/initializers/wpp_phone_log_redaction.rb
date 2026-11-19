# frozen_string_literal: true

# Keep Chatwoot's upstream security filters and add phone-specific filtering.
# Rails::Rack::Logger uses filtered_path for `Started` logs, while controller
# parameter logs use filtered_parameters; both read this same filter list.
module WppAiBot
  module PhoneLogRedaction
    REDACTION = '[FILTERED]'
    DIGIT = '(?:[0-9]|%3[0-9])'
    SEPARATOR = '(?:[ +.()\-]|%(?:20|28|29|2d|2e))*'
    PHONE_LIKE = Regexp.new(
      "(?<![[:alnum:]])(?:\\+|%2b)?#{DIGIT}(?:#{SEPARATOR}#{DIGIT}){7,14}(?![[:alnum:]])",
      Regexp::IGNORECASE
    )

    VALUE_FILTER = lambda do |_key, value|
      value.gsub!(PHONE_LIKE, REDACTION) if value.is_a?(String)
    end
  end
end

Rails.application.config.filter_parameters += [
  /\Aq\z/i,
  /\Aphone\z/i,
  /\Aphone_number\z/i,
  /\A(content|body|message|summary|resumo)\z/i,
  /\A(authorization|api_access_token|token|secret|signature)\z/i,
  WppAiBot::PhoneLogRedaction::VALUE_FILTER
]

# Webhook jobs carry the complete provider payload plus the signing secret.
# Rails' normal controller filter does not cover Active Job argument logging.
Rails.application.config.active_job.log_arguments = false
