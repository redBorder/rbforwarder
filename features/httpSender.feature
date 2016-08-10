Feature: http sender
  In order to send data to an endpoint
  As a pipeline component
  I need to implement an HTTP client to send HTTP messages

  Scenario: Initialization
    Given an HTTP sender with defined URL
    When is initialized
    Then the config should be ok

  Scenario: Message without endpoint
    Given an HTTP sender with defined URL
    When a message is received without endpoint option
    Then the message should be sent via HTTP to the URL

  Scenario: Message with endpoint
    Given an HTTP sender with defined URL
    When a message is received with endpoint option
    Then the message should be sent to the URL with endpoint as suffix

  Scenario: Message with no data
    Given an HTTP sender with defined URL
    When a message without data is received
    Then the message should not be sent

  Scenario: HTTP Sender with no configuration
    Given an HTTP sender without URL
    When is initialized
    Then should fail
