#!/usr/bin/ruby
require 'rubygems'
require 'memcache'
require 'socket'

def local_ip
  orig, Socket.do_not_reverse_lookup = Socket.do_not_reverse_lookup, true  # turn off reverse DNS resolution temporarily

  UDPSocket.open do |s|
    s.connect '64.233.187.99', 1
    s.addr.last
  end
ensure
  Socket.do_not_reverse_lookup = orig
end

 ip = local_ip + ":11212"
puts "Connecting to gemcached server on:" + ip
 
cache = MemCache::new ip,
                       :debug => false
                       #:c_threshold => 100_000,
                       #:compression => false
  #cache.servers += [ "10.0.0.15:11211:5" ]
  #cache.c_threshold = 10_000
  #cache.compression = true

  # Cache simple values with simple String or Symbol keys
  
  cache["my_key"] = "Some value"
  cache[:other_key] = "Another value"

  # ...or more-complex values
  cache["object_key"] = { 'complex' => [ "object", 2, 4 ] }

  # ...or more-complex keys
  cache[ Time::now.to_a[1..7] ] ||= 0

  # ...or both
  #cache[userObject] = { :attempts => 0, :edges => [], :nodes => [] }

  raise "value did not match" if (cache["my_key"] <=> "Some value") !=0               # => "Some value"
  val = cache["my_key"]
  puts val
  val = cache["object_key"]           # => {"complex" => ["object",2,4]}
  puts val
  print val['complex'][2]             # => 4
  raise "value did not match" if !val['complex'][2].equal?(4)