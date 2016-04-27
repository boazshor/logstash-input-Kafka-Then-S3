require 'logstash/namespace'
require 'logstash/inputs/base'
require 'jruby-kafka'
require 'logstash/plugin_mixins/aws_config'
require "time"
require "tmpdir"
require "stud/interval"
require "stud/temporary"
require "json"

# This input will read events from a Kafka topic. It uses the high level consumer API provided
# by Kafka to read messages from the broker. It also maintains the state of what has been
# consumed using Zookeeper. The default input codec is json
#
# You must configure `topic_id`, `white_list` or `black_list`. By default it will connect to a
# Zookeeper running on localhost. All the broker information is read from Zookeeper state
#
# Ideally you should have as many threads as the number of partitions for a perfect balance --
# more threads than partitions means that some threads will be idle
#
# For more information see http://kafka.apache.org/documentation.html#theconsumer
#
# Kafka consumer configuration: http://kafka.apache.org/documentation.html#consumerconfigs
#
class LogStash::Inputs::KafkaThenS3 < LogStash::Inputs::Base
  include LogStash::PluginMixins::AwsConfig
  config_name 'kafka_then_s3'

  default :codec, 'plain'

  # Specifies the ZooKeeper connection string in the form hostname:port where host and port are
  # the host and port of a ZooKeeper server. You can also specify multiple hosts in the form
  # `hostname1:port1,hostname2:port2,hostname3:port3`.
  #
  # The server may also have a ZooKeeper chroot path as part of it's ZooKeeper connection string
  # which puts its data under some path in the global ZooKeeper namespace. If so the consumer
  # should use the same chroot path in its connection string. For example to give a chroot path of
  # `/chroot/path` you would give the connection string as
  # `hostname1:port1,hostname2:port2,hostname3:port3/chroot/path`.
  config :zk_connect, :validate => :string, :default => 'localhost:2181'
  # A string that uniquely identifies the group of consumer processes to which this consumer
  # belongs. By setting the same group id multiple processes indicate that they are all part of
  # the same consumer group.
  config :group_id, :validate => :string, :default => 'logstash'
  # The topic to consume messages from
  config :topic_id, :validate => :string, :default => nil
  # Whitelist of topics to include for consumption.
  config :white_list, :validate => :string, :default => nil
  # Blacklist of topics to exclude from consumption.
  config :black_list, :validate => :string, :default => nil
  # Reset the consumer group to start at the earliest message present in the log by clearing any
  # offsets for the group stored in Zookeeper. This is destructive! Must be used in conjunction
  # with auto_offset_reset => 'smallest'
  config :reset_beginning, :validate => :boolean, :default => false
  # `smallest` or `largest` - (optional, default `largest`) If the consumer does not already
  # have an established offset or offset is invalid, start with the earliest message present in the
  # log (`smallest`) or after the last message in the log (`largest`).
  config :auto_offset_reset, :validate => %w( largest smallest ), :default => 'largest'
  # The frequency in ms that the consumer offsets are committed to zookeeper.
  config :auto_commit_interval_ms, :validate => :number, :default => 1000
  # Number of threads to read from the partitions. Ideally you should have as many threads as the
  # number of partitions for a perfect balance. More threads than partitions means that some
  # threads will be idle. Less threads means a single thread could be consuming from more than
  # one partition
  config :consumer_threads, :validate => :number, :default => 1
  # Internal Logstash queue size used to hold events in memory after it has been read from Kafka
  config :queue_size, :validate => :number, :default => 20
  # When a new consumer joins a consumer group the set of consumers attempt to "rebalance" the
  # load to assign partitions to each consumer. If the set of consumers changes while this
  # assignment is taking place the rebalance will fail and retry. This setting controls the
  # maximum number of attempts before giving up.
  config :rebalance_max_retries, :validate => :number, :default => 4
  # Backoff time between retries during rebalance.
  config :rebalance_backoff_ms, :validate => :number, :default => 2000
  # Throw a timeout exception to the consumer if no message is available for consumption after
  # the specified interval
  config :consumer_timeout_ms, :validate => :number, :default => -1
  # Option to restart the consumer loop on error
  config :consumer_restart_on_error, :validate => :boolean, :default => true
  # Time in millis to wait for consumer to restart after an error
  config :consumer_restart_sleep_ms, :validate => :number, :default => 0
  # Option to add Kafka metadata like topic, message size to the event.
  # This will add a field named `kafka` to the logstash event containing the following attributes:
  #   `msg_size`: The complete serialized size of this message in bytes (including crc, header attributes, etc)
  #   `topic`: The topic this message is associated with
  #   `consumer_group`: The consumer group used to read in this event
  #   `partition`: The partition this message is associated with
  #   `offset`: The offset from the partition this message is associated with
  #   `key`: A ByteBuffer containing the message key
  config :decorate_events, :validate => :boolean, :default => false
  # A unique id for the consumer; generated automatically if not set.
  config :consumer_id, :validate => :string, :default => nil
  # The number of byes of messages to attempt to fetch for each topic-partition in each fetch
  # request. These bytes will be read into memory for each partition, so this helps control
  # the memory used by the consumer. The fetch request size must be at least as large as the
  # maximum message size the server allows or else it is possible for the producer to send
  # messages larger than the consumer can fetch.
  config :fetch_message_max_bytes, :validate => :number, :default => 1048576
  # The serializer class for messages. The default decoder takes a byte[] and returns the same byte[]
  config :decoder_class, :validate => :string, :default => 'kafka.serializer.DefaultDecoder'
  # The serializer class for keys (defaults to the same default as for messages)
  config :key_decoder_class, :validate => :string, :default => 'kafka.serializer.DefaultDecoder'
  # DEPRECATED: The credentials of the AWS account used to access the bucket.
  # Credentials can be specified:
  # - As an ["id","secret"] array
  # - As a path to a file containing AWS_ACCESS_KEY_ID=... and AWS_SECRET_ACCESS_KEY=...
  # - In the environment, if not set (using variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY)
  config :credentials, :validate => :array, :default => [], :deprecated => "This only exists to be backwards compatible. This plugin now uses the AwsConfig from PluginMixins"

  # The name of the S3 bucket.
  config :bucket, :validate => :string, :required => true

  # The AWS region for your bucket.
  config :region_endpoint, :validate => ["us-east-1", "us-west-1", "us-west-2",
                                         "eu-west-1", "ap-southeast-1", "ap-southeast-2",
                                         "ap-northeast-1", "sa-east-1", "us-gov-west-1"], :deprecated => "This only exists to be backwards compatible. This plugin now uses the AwsConfig from PluginMixins"

  # If specified, the prefix of filenames in the bucket must match (not a regexp)
  config :prefix, :validate => :string, :default => nil

  # Where to write the since database (keeps track of the date
  # the last handled file was added to S3). The default will write
  # sincedb files to some path matching "$HOME/.sincedb*"
  # Should be a path with filename not just a directory.
  config :sincedb_path, :validate => :string, :default => nil

  # Name of a S3 bucket to backup processed files to.
  config :backup_to_bucket, :validate => :string, :default => nil

  # Append a prefix to the key (full path including file name in s3) after processing.
  # If backing up to another (or the same) bucket, this effectively lets you
  # choose a new 'folder' to place the files in
  config :backup_add_prefix, :validate => :string, :default => nil

  # Path of a local directory to backup processed files to.
  config :backup_to_dir, :validate => :string, :default => nil

  # Whether to delete processed files from the original bucket.
  config :delete, :validate => :boolean, :default => false

  # Interval to wait between to check the file list again after a run is finished.
  # Value is in seconds.
  config :interval, :validate => :number, :default => 60

  # Ruby style regexp of keys to exclude from the bucket
  config :exclude_pattern, :validate => :string, :default => nil

  # Set the directory where logstash will store the tmp files before processing them.
  # default to the current OS temporary directory in linux /tmp/logstash
  config :temporary_directory, :validate => :string, :default => File.join(Dir.tmpdir, "logstash")

  class KafkaShutdownEvent; end
  KAFKA_SHUTDOWN_EVENT = KafkaShutdownEvent.new

  public
  def register
    require "fileutils"
    require "digest/md5"
    require "aws-sdk"

    @region = get_region

    @logger.info("Registering kafkaThenS3 input", :bucket => @bucket, :region => @region)

    s3 = get_s3object
    @logger.debug("the bucket name in register is : " + @bucket +"\n")
    @s3bucket = s3.buckets[@bucket]


    unless @backup_to_bucket.nil?
      @backup_bucket = s3.buckets[@backup_to_bucket]
      unless @backup_bucket.exists?
        s3.buckets.create(@backup_to_bucket)
      end
    end

    unless @backup_to_dir.nil?
      Dir.mkdir(@backup_to_dir, 0700) unless File.exists?(@backup_to_dir)
    end

    FileUtils.mkdir_p(@temporary_directory) unless Dir.exist?(@temporary_directory)

    LogStash::Logger.setup_log4j(@logger)
    options = {
        :zk_connect => @zk_connect,
        :group_id => @group_id,
        :topic_id => @topic_id,
        :auto_offset_reset => @auto_offset_reset,
        :auto_commit_interval => @auto_commit_interval_ms,
        :rebalance_max_retries => @rebalance_max_retries,
        :rebalance_backoff_ms => @rebalance_backoff_ms,
        :consumer_timeout_ms => @consumer_timeout_ms,
        :consumer_restart_on_error => @consumer_restart_on_error,
        :consumer_restart_sleep_ms => @consumer_restart_sleep_ms,
        :consumer_id => @consumer_id,
        :fetch_message_max_bytes => @fetch_message_max_bytes,
        :allow_topics => @white_list,
        :filter_topics => @black_list,
        :value_decoder_class => @decoder_class,
        :key_decoder_class => @key_decoder_class
    }
    if @reset_beginning
      options[:reset_beginning] = 'from-beginning'
    end # if :reset_beginning
    topic_or_filter = [@topic_id, @white_list, @black_list].compact
    if topic_or_filter.count == 0
      raise LogStash::ConfigurationError, 'topic_id, white_list or black_list required.'
    elsif topic_or_filter.count > 1
      raise LogStash::ConfigurationError, 'Invalid combination of topic_id, white_list or black_list. Use only one.'
    end
    @kafka_client_queue = SizedQueue.new(@queue_size)
    @consumer_group = create_consumer_group(options)
    @logger.info('Registering kafka', :group_id => @group_id, :topic_id => @topic_id, :zk_connect => @zk_connect)

  end # def register

  public
  def run(logstash_queue)
    # noinspection JRubyStringImportInspection
    java_import 'kafka.common.ConsumerRebalanceFailedException'
    @logger.info('Running kafka', :group_id => @group_id, :topic_id => @topic_id, :zk_connect => @zk_connect)
    begin
      @consumer_group.run(@consumer_threads,@kafka_client_queue)
      while !stop?
        event = @kafka_client_queue.pop
        if event == KAFKA_SHUTDOWN_EVENT
          break
        end
        queue_event(event, logstash_queue)
      end

      until @kafka_client_queue.empty?
        queue_event(@kafka_client_queue.pop,logstash_queue)
      end

      @logger.info('Done running kafka input')
    rescue => e
      @logger.warn('kafka client threw exception, restarting',
                   :exception => e)
      Stud.stoppable_sleep(Float(@consumer_restart_sleep_ms) * 1 / 1000) { stop? }
      retry if !stop?
    end
  end # def run

  public
  def stop
    @kafka_client_queue.push(KAFKA_SHUTDOWN_EVENT)
    @consumer_group.shutdown if @consumer_group.running?
  end

  private
  def create_consumer_group(options)
    Kafka::Group.new(options)
  end

  private
  def queue_event(message_and_metadata, output_queue)
    begin
      @codec.decode("#{message_and_metadata.message}") do |event|
        decorate(event)
        # if @decorate_events
        #   event['kafka'] = {'msg_size' => message_and_metadata.message.size,
        #                     'topic' => message_and_metadata.topic,
        #                     'consumer_group' => @group_id,
        #                     'partition' => message_and_metadata.partition,
        #                     'offset' => message_and_metadata.offset,
        #                     'key' => message_and_metadata.key,
        #                     'messge' => message_and_metadata.message}
        msg = JSON.parse(event["message"])
       @logger.debug( "after json.parse : "+ msg.to_s)
        @logger.warn('the msg from kafka',:message => "#{event['message']}")

        process_files(msg, output_queue)
      end


      # end # @codec.decode
    rescue => e # parse or event creation error
      @logger.error('Failed to create event', :message => "#{message_and_metadata.message}", :exception => e,
                    :backtrace => e.backtrace)
    end # begin
  end # def queue_event
  # public


  public
  def backup_to_bucket(object, key)
    unless @backup_to_bucket.nil?
      backup_key = "#{@backup_add_prefix}#{key}"
      if @delete
        object.move_to(backup_key, :bucket => @backup_bucket)
      else
        object.copy_to(backup_key, :bucket => @backup_bucket)
      end
    end
  end

  public
  def backup_to_dir(filename)
    unless @backup_to_dir.nil?
      FileUtils.cp(filename, @backup_to_dir)
    end
  end

  public
  def process_files(event ,queue)
        @logger.info("S3 input processing", :bucket => @bucket, :key => event["filename"])
        process_log(queue, event)


  end # def process_files

  public
  def stop
    # @current_thread is initialized in the `#run` method,
    # this variable is needed because the `#stop` is a called in another thread
    # than the `#run` method and requiring us to call stop! with a explicit thread.
    Stud.stop!(@current_thread)
  end

  public
  def aws_service_endpoint(region)
    region_to_use = get_region

    return {
        :s3_endpoint => region_to_use == 'us-east-1' ?
            's3.amazonaws.com' : "s3-#{region_to_use}.amazonaws.com"
    }
  end

  private

  # Read the content of the local file
  #
  # @param [Queue] Where to push the event
  # @param [String] Which file to read from
  # @return [Boolean] True if the file was completely read, false otherwise.
  def process_local_log(queue, filename, origEvent)
   @logger.debug( "Processing file " + filename)
    metadata = {}
    # Currently codecs operates on bytes instead of stream.
    # So all IO stuff: decompression, reading need to be done in the actual
    # input and send as bytes to the codecs.
    read_file(filename) do |line, isEof|

      if stop?
        @logger.warn("Logstash S3 input, stop reading in the middle of the file, we will read it again when logstash is started")
        return false
      end
      @codec.decode(line) do |event|
        # We are making an assumption concerning cloudfront
        # log format, the user will use the plain or the line codec
        # and the message key will represent the actual line content.
        # If the event is only metadata the event will be drop.
        # This was the behavior of the pre 1.5 plugin.
        #
        # The line need to go through the codecs to replace
        # unknown bytes in the log stream before doing a regexp match or
        # you will get a `Error: invalid byte sequence in UTF-8'
        if event_is_metadata?(event)
          @logger.warn('Event is metadata, updating the current cloudfront metadata', :event => event)
          update_metadata(metadata, event)
        else
          decorate(event)
          # printf "the eof is " + isEof + "\n"
          event["isEof"] = isEof
          event["context"] = origEvent['context']
          queue << event
        end
      end
    end

    return true
  end # def process_local_log

  private
  def event_is_metadata?(event)
    return false if event["message"].nil?
    line = event["message"]
    version_metadata?(line) || fields_metadata?(line)
  end

  private
  def version_metadata?(line)
    line.start_with?('#Version: ')
  end

  private
  def fields_metadata?(line)
    line.start_with?('#Fields: ')
  end

  private
  def update_metadata(metadata, event)
    line = event['message'].strip

    if version_metadata?(line)
      metadata[:cloudfront_version] = line.split(/#Version: (.+)/).last
    end

    if fields_metadata?(line)
      metadata[:cloudfront_fields] = line.split(/#Fields: (.+)/).last
    end
  end

  private
  def read_file(filename, &block)
    if gzip?(filename)
      read_gzip_file(filename, block)
    else
      read_plain_file(filename, block)
    end
  end


  def read_plain_file(filename, block)
   @logger.debug( "in read plain file , the fileName is : "+filename + "\n")
    File.open(filename, 'rb') do |file|
     # printf "in file open,is EOF : " + file.eof? + "\n"
     file.each {|line| block.call(line,file.eof?)}
    end
  end

  private
  def read_gzip_file(filename, block)
    begin
      Zlib::GzipReader.open(filename) do |decoder|
        decoder.each_line { |line| block.call(line) }
      end
    rescue Zlib::Error, Zlib::GzipFile::Error => e
      @logger.error("Gzip codec: We cannot uncompress the gzip file", :filename => filename)
      raise e
    end
  end

  private
  def gzip?(filename)
    filename.end_with?('.gz')
  end

  private
  def sincedb
    @sincedb ||= if @sincedb_path.nil?
                   @logger.info("Using default generated file for the sincedb", :filename => sincedb_file)
                   SinceDB::File.new(sincedb_file)
                 else
                   @logger.info("Using the provided sincedb_path",
                                :sincedb_path => @sincedb_path)
                   SinceDB::File.new(@sincedb_path)
                 end
  end

  private
  def sincedb_file
    File.join(ENV["HOME"], ".sincedb_" + Digest::MD5.hexdigest("#{@bucket}+#{@prefix}"))
  end

  private
  def ignore_filename?(filename)
    if @prefix == filename
      return true
    elsif (@backup_add_prefix && @backup_to_bucket == @bucket && filename =~ /^#{backup_add_prefix}/)
      return true
    elsif @exclude_pattern.nil?
      return false
    elsif filename =~ Regexp.new(@exclude_pattern)
      return true
    else
      return false
    end
  end

  private
  def process_log(queue, event)
    key = event["context"]["filename"]
   @logger.debug( "ant the key is : "+key + "\n")
    object = @s3bucket.objects[key]
   @logger.debug( "S3 bucket returns : "+ object.to_s + "\n")
    filename = File.join(temporary_directory, File.basename(key))
   @logger.debug( "file name is : "+ filename + "\n")
    if download_remote_file(object, filename)
     @logger.debug( "after save to local \n")
      if process_local_log(queue, filename, event)
        lastmod = object.last_modified
        backup_to_bucket(object, key)
        backup_to_dir(filename)
        delete_file_from_bucket(object)
        FileUtils.remove_entry_secure(filename, true)
        sincedb.write(lastmod)
      end
    else
      FileUtils.remove_entry_secure(filename, true)
    end
  end

  private
  # Stream the remove file to the local disk
  #
  # @param [S3Object] Reference to the remove S3 objec to download
  # @param [String] The Temporary filename to stream to.
  # @return [Boolean] True if the file was completely downloaded
  def download_remote_file(remote_object, local_filename)
    # cont = remote_object.read
#" the content is  : " + cont.to_s + "\n"))
    completed = false
    @logger.warn("S3 input: Download remote file", :remote_key => remote_object.key, :local_filename => local_filename)
    File.open(local_filename, 'wb') do |s3file|

     @logger.debug( "before remote.read " + remote_object.to_s )
      remote_object.read do |chunk|
       @logger.debug( "In remote object read, the chunk is : " + chunk.to_s)
        return completed if stop?
        s3file.write(chunk)
      end
    end
    completed = true

    return completed
  end

  private
  def delete_file_from_bucket(object)
    if @delete and @backup_to_bucket.nil?
      object.delete()
    end
  end

  private
  def get_region
    # TODO: (ph) Deprecated, it will be removed
    if @region_endpoint
      @region_endpoint
    else
      @region
    end
  end

  private
  def get_s3object
    # TODO: (ph) Deprecated, it will be removed
    if @credentials.length == 1
      File.open(@credentials[0]) { |f| f.each do |line|
        unless (/^\#/.match(line))
          if(/\s*=\s*/.match(line))
            param, value = line.split('=', 2)
            param = param.chomp().strip()
            value = value.chomp().strip()
            if param.eql?('AWS_ACCESS_KEY_ID')
              @access_key_id = value
            elsif param.eql?('AWS_SECRET_ACCESS_KEY')
              @secret_access_key = value
            end
          end
        end
      end
      }
    elsif @credentials.length == 2
      @access_key_id = @credentials[0]
      @secret_access_key = @credentials[1]
    end

    s3 = AWS::S3.new()
  end

  private
  module SinceDB
    class File
      def initialize(file)
        @sincedb_path = file
      end

      def newer?(date)
        date > read
      end

      def read
        if ::File.exists?(@sincedb_path)
          content = ::File.read(@sincedb_path).chomp.strip
          # If the file was created but we didn't have the time to write to it
          return content.empty? ? Time.new(0) : Time.parse(content)
        else
          return Time.new(0)
        end
      end

      def write(since = nil)
        since = Time.now() if since.nil?
        ::File.open(@sincedb_path, 'w') { |file| file.write(since.to_s) }
      end
    end
  end
end #class LogStash::Inputs::KafkaThenS3
