# encoding: utf-8
# require "logstash/spec/spec_helper"
require "logstash/inputs/kafka_then_s3"
require 'jruby-kafka'
require "logstash/errors"
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
    attr_reader :topic, :partition, :key, :message, :offset, :bucket
    def initialize(topic, partition, key, message, offset)
      @topic = topic
      @partition = partition
      @key = key
      @message = message
      @offset = offset
      @bucket = bucket
    end
  end


  class TestKafkaGroup < Kafka::Group
    def run(a_num_threads, a_queue)
      blah = TestMessageAndMetadata.new(@topic, 0, nil, "{\"context\":{\"filename\":\"simpleTextFile.txt\"}}", 1)
      a_queue << blah
    end
  end
class TestKafkaGZGroup < Kafka::Group
    def run(a_num_threads, a_queue)
      blah = TestMessageAndMetadata.new(@topic, 0, nil, "{\"context\":{\"filename\":\"simpleTextFile.gz\"}}", 1)
      a_queue << blah
    end
  end

  class LogStash::Inputs::TestInfiniteKafka < LogStash::Inputs::KafkaThenS3
    private
    def queue_event(msg, output_queue)
      super(msg, output_queue)
    end
  end

  class TestInfiniteKafkaGroup < Kafka::Group
    def run(a_num_threads, a_queue)
      blah = TestMessageAndMetadata.new(@topic, 0, nil,  "{\"context\":{\"filename\":\"kafka.message\"}}", 1)
      Thread.new do
        while true
          a_queue << blah
          sleep 10
        end
      end
    end
  end



  describe LogStash::Inputs::KafkaThenS3 do
    before (:each) do
      AWS.stub!
      Thread.abort_on_exception = true
      end


    let (:kafka_config) {{'topic_id' => 'test',
                          'bucket' => 'test_bucket',
                          "access_key_id" => "1234",
                          "secret_access_key" => "secret",
                          "bucket" => "logstash-test",
                          "temporary_directory" => temporary_directory,
                          "sincedb_path" => File.join(sincedb_path, ".sincedb"),
                          "isEOF" => true
    }}
    let (:min_config) {{'bucket' => 'test_bucket'}}
    let (:bad_kafka_config) {{'topic_id' => 'test',
                              'white_list' => 'other_topic',
                              'bucket' => 'test_bucket'
    }}
    let (:white_list_kafka_config) {{'white_list' => 'other_topic','bucket' => 'test_bucket'}}

    let(:temporary_directory) { Stud::Temporary.pathname }
    let(:sincedb_path) { Stud::Temporary.pathname }
    let(:day) { 3600 * 24 }
    let(:temporary_directory) { Stud::Temporary.pathname }
    let(:prefix)  { 'logstash-s3-input-prefix/' }

    it_behaves_like "an interruptible input plugin" do
      let(:config) { kafka_config }
      let(:mock_kafkaThenS3_plugin) { LogStash::Inputs::TestInfiniteKafka.new(config) }


      before do
        LogStash::Inputs::KafkaThenS3.any_instance.stub(:new).and_return(LogStash::Inputs::TestInfiniteKafka.new(config))
        expect(subject).to receive(:create_consumer_group) do |options|
          TestInfiniteKafkaGroup.new(options)
        end
      end
    end

    it "should register" do
      FileUtils.rm_rf(temporary_directory)
      input = LogStash::Plugin.lookup("input", "kafka_then_s3").new(kafka_config)
      expect {input.register}.to_not raise_error
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


    it 'should populate kafkaThenS3 config with default values' do
      kafka = LogStash::Inputs::TestKafkaThenS3.new(kafka_config)
      insist {kafka.zk_connect} == 'localhost:2181'
      insist {kafka.topic_id} == 'test'
      insist {kafka.group_id} == 'logstash'
      !insist { kafka.reset_beginning }
    end

    it 'should retrieve event from kafka and fetch a plain text file from S3' do
      # LogStash::Inputs::TestKafkaThenS3.any_instance.should_receive(:new)
      simpleTextFileContent = 'simple file from s3'
      AWS::S3::S3Object.any_instance.stub(:read).and_yield(simpleTextFileContent)
      kafka = LogStash::Inputs::TestKafkaThenS3.new(kafka_config)
      expect(kafka).to receive(:create_consumer_group) do |options|
        TestKafkaGroup.new(options)
      end
      kafka.register

      logstash_queue = Queue.new
      kafka.run logstash_queue
      e = logstash_queue.pop
      expect(e['context']).to be_truthy
      # expect(e['isEof']).to eq(true)
      insist { e['message'] } == simpleTextFileContent
      # no metadata by default
      insist { e['kafka'] } == nil
    end
    #
    it 'should retrieve event from kafka and fetch a gzip file from S3' do
      fileName = File.expand_path(File.join(File.dirname(__FILE__), '../resources/simpleTextFile.gz'))
      AWS::S3::S3Object.any_instance.stub(:read).and_yield(File.open(fileName).read)
      kafka = LogStash::Inputs::TestKafkaThenS3.new(kafka_config)
      expect(kafka).to receive(:create_consumer_group) do |options|
        TestKafkaGZGroup.new(options)
      end
      kafka.register

      logstash_queue = Queue.new
      kafka.run logstash_queue
      e = logstash_queue.pop

      expect(e['context']).to be_truthy
      # expect(e['isEof']).to eq(true)

      insist {e['message'] } == "simple file from s3\n"
      # no metadata by default
      insist { e['kafka'] } == nil
    end





  end


