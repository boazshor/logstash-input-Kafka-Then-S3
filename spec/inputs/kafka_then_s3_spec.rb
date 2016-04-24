# encoding: utf-8
# require "logstash/spec/spec_helper"
require "logstash/inputs/kafka_then_s3"
require 'jruby-kafka'
require "logstash/errors"
require_relative "../support/helpers"
require "stud/temporary"
require "aws-sdk"
require "fileutils"

class LogStash::Inputs::TestKafkaThenS3 < LogStash::Inputs::KafkaThenS3
    private
    def queue_event(msg, output_queue)
      super(msg, output_queue)
      do_stop
    end
  end

  class TestMessageAndMetadata
    attr_reader :topic, :partition, :key, :message, :offset
    def initialize(topic, partition, key, message, offset)
      @topic = topic
      @partition = partition
      @key = key
      @message = message
      @offset = offset
    end
  end


  class TestKafkaGroup < Kafka::Group
    def run(a_num_threads, a_queue)
      blah = TestMessageAndMetadata.new(@topic, 0, nil, 'Kafka message', 1)
      a_queue << blah
    end
  end

  class LogStash::Inputs::TestInfiniteKafka < LogStash::Inputs::TestKafkaThenS3
    private
    def queue_event(msg, output_queue)
      super(msg, output_queue)
    end

    def each
      counter = 1

      loop do
        yield "awesome-#{counter}"
        counter +=1
      end
    end
  end

  class TestInfiniteKafkaGroup < Kafka::Group
    def run(a_num_threads, a_queue)
      blah = TestMessageAndMetadata.new(@topic, 0, nil, 'Kafka message', 1)
      Thread.new do
        while true
          a_queue << blah
          sleep 10
        end
      end
    end
  end



  describe LogStash::Inputs::TestKafkaThenS3 do
       before do
         FileUtils.mkdir_p(sincedb_path)
         AWS.stub!
         Thread.abort_on_exception = true
       end
       context "when interrupting the plugin" do
         let(:config) { super.merge({ "interval" => 5 }) }

         before do
           expect_any_instance_of(LogStash::Inputs::KafkaThenS3).to receive(:list_new_files).and_return(TestKafkaThenS3.new)
         end

         it_behaves_like "an interruptible input plugin"
       end



       after do
         delete_remote_files(prefix)
         FileUtils.rm_rf(temporary_directory)
         delete_remote_files(backup_prefix)
       end


    let (:kafka_config) {{'topic_id' => 'test',
                          'bucket' => 'test_bucket',
                          "access_key_id" => "1234",
                          "secret_access_key" => "secret",
                          "bucket" => "logstash-test",
                          "temporary_directory" => temporary_directory,
                          "sincedb_path" => File.join(sincedb_path, ".sincedb")
    }}
    let (:min_config) {{'bucket' => 'test_bucket'}}
    let (:bad_kafka_config) {{'topic_id' => 'test',
                              'white_list' => 'other_topic',
                              'bucket' => 'test_bucket'
    }}
    let (:white_list_kafka_config) {{'white_list' => 'other_topic','bucket' => 'test_bucket'}}

    #let (:decorated_kafka_config) {{'topic_id' => 'test', 'decorate_events' => true}}
    let(:temporary_directory) { Stud::Temporary.pathname }
    let(:sincedb_path) { Stud::Temporary.pathname }
    let(:day) { 3600 * 24 }
    let(:temporary_directory) { Stud::Temporary.pathname }
    let(:prefix)  { 'logstash-s3-input-prefix/' }
    it "should register" do
      FileUtils.rm_rf(temporary_directory)
      input = LogStash::Plugin.lookup("input", "kafka_then_s3").new(kafka_config)
      expect {input.register}.to_not raise_error
      # expect { input.register }.to change { Dir.exist?(temporary_directory) }.from(false).to(true)
      FileUtils.rm_rf(temporary_directory)
    end

    it "should register with whitelist" do
      input = LogStash::Plugin.lookup("input", "kafka_then_s3").new(white_list_kafka_config)
      expect {input.register}.to_not raise_error
    end

    it "should fail with multiple topic configs" do
      input = LogStash::Plugin.lookup("input", "kafka_then_s3").new(bad_kafka_config)
      expect {input.register}.to raise_error
    end

    it "should fail without topic configs" do
      input = LogStash::Plugin.lookup("input", "kafka_then_s3").new(min_config)
      expect {input.register}.to raise_error
    end



    it_behaves_like "an interruptible input plugin" do
      let(:config) { kafka_config }
      let(:mock_kafka_plugin) { LogStash::Inputs::TestInfiniteKafka.new(config) }

      before :each do
        allow(LogStash::Inputs::TestKafkaThenS3).to receive(:new).and_return(mock_kafka_plugin)
        expect(subject).to receive(:create_consumer_group) do |options|
          TestInfiniteKafkaGroup.new(options)
        end
      end
    end

    it 'should populate kafka config with default values' do
      kafka = LogStash::Inputs::TestKafkaThenS3.new(kafka_config)
      insist {kafka.zk_connect} == 'localhost:2181'
      insist {kafka.topic_id} == 'test'
      insist {kafka.group_id} == 'logstash'
      !insist { kafka.reset_beginning }
    end

    # it 'should retrieve event from kafka' do
    #   kafka = LogStash::Inputs::TestKafkaThenS3.new(kafka_config)
    #   expect(kafka).to receive(:create_consumer_group) do |options|
    #     TestKafkaGroup.new(options)
    #   end
    #   kafka.register
    #
    #   logstash_queue = Queue.new
    #   kafka.run logstash_queue
    #   e = logstash_queue.pop
    #   insist { e['message'] } == 'Kafka message'
    #   # no metadata by default
    #   insist { e['kafka'] } == nil
    # end


  end


