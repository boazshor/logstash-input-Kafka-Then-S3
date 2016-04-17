Gem::Specification.new do |s|

  s.name            = 'logstash-input-kafka_then_s3'
  s.version         = '1.0.0'
  s.licenses        = ['Apache License (2.0)']
  s.summary         = 'This input will read events from a Kafka topic an s3 key and reads the value of the s3 key from the s3 bucket. It uses the high level consumer API provided by Kafka to read messages from the broker'
  s.description     = "This gem is a logstash plugin required to be installed on top of the Logstash core pipeline using $LS_HOME/bin/plugin install gemname. This gem is not a stand-alone program"
  s.authors         = ['Boaz Shor']
  s.email           = 'boaz.shor@hpe.com'
  s.homepage        = "http://www.elastic.co/guide/en/logstash/current/index.html"
  s.require_paths = ['lib']

  # Files
  s.files = Dir['lib/**/*','spec/**/*','vendor/**/*','*.gemspec','*.md','CONTRIBUTORS','Gemfile','LICENSE','NOTICE.TXT']

  # Tests
  s.test_files = s.files.grep(%r{^(test|spec|features)/})

  # Special flag to let us know this is actually a logstash plugin
  s.metadata = { 'logstash_plugin' => 'true', 'group' => 'input'}

  # Gem dependencies
  s.add_runtime_dependency "logstash-core-plugin-api", "~> 1.0"
  s.add_development_dependency 'logstash-devutils'
  s.add_runtime_dependency 'logstash-codec-json'
  s.add_runtime_dependency 'logstash-codec-plain'
  s.add_runtime_dependency 'logstash-mixin-aws'
  s.add_runtime_dependency 'stud', '>= 0.0.22', '< 0.1.0'
  s.add_runtime_dependency 'jruby-kafka', '1.5.0'
  s.add_development_dependency 'simplecov'
  s.add_development_dependency 'coveralls'
  s.add_development_dependency 'json'

end

