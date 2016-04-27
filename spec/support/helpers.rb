

# delete_files(prefix)
def upload_file(local_file, remote_name)
  bucket = s3object.buckets[ENV['AWS_LOGSTASH_TEST_BUCKET']]
   printf "in uploadfile, the bucket is : " + bucket.name + " the local file is : "+local_file + " and the remote name is :  " + remote_name + "\n"
  file = File.expand_path(File.join(File.dirname(__FILE__), local_file))
  splitFile = file.split('C:')
  truePath = 'C:' + splitFile[1]
   printf "the full filename is : " + truePath + "\n"
  bucket.objects[remote_name].write(:file => truePath)

end

def delete_remote_files(prefix)
  bucket = s3object.buckets[ENV['AWS_LOGSTASH_TEST_BUCKET']]
  bucket.objects.with_prefix(prefix).each { |object| object.delete }
end

# def list_remote_files(prefix, target_bucket = ENV['AWS_LOGSTASH_TEST_BUCKET'])
#   bucket = s3object.buckets[target_bucket]
#   bucket.objects.with_prefix(prefix).collect(&:key)
# end

def delete_bucket(name)
  s3object.buckets[name].objects.map(&:delete)
  s3object.buckets[name].delete
end

def s3object
  AWS::S3.new
end

# class TestInfiniteS3Object
#   def each
#     counter = 1
#
#     loop do
#       yield "awesome-#{counter}"
#       counter +=1
#     end
#   end
# end

