require "digest/sha1"
class Redis
  module BloomfilterDriver

    # It loads lua script into redis.
    # BF implementation is done by lua scripting
    # The alghoritm is executed directly on redis
    # Credits for lua code goes to Erik Dubbelboer
    # https://github.com/ErikDubbelboer/redis-lua-scaling-bloom-filter
    class Lua
      attr_accessor :redis

      def initialize(options = {})
        @options = options
        @redis = @options[:redis]
        lua_load
      end

      def insert(data)
        set(data, 1) == 1
      end

      def insertnx(data)
        setnx(data, 1) == 1
      end

      def remove(data)
        set(data, 0) == 1
      end

      def include?(data)
        call("check", data) == 1
      end

      def clear
        @redis.keys("#{@options[:key_name]}:*").each {|k|@redis.del k}
      end

      protected
        # It loads the script inside Redis
        # Taken from https://github.com/ErikDubbelboer/redis-lua-scaling-bloom-filter
        # This is a scalable implementation of BF. It means the initial size can vary
        def lua_load
          functions = %q(
            local functions = {}
            functions.check = function(key, entries, precision, data)
              local keyct = key .. ':count'
              local index     = redis.call('GET', keyct)
              if not index then
                return 0
              end
              index = math.ceil(index / entries)
              local hash = redis.sha1hex(data)
              local h = { }
              h[0] = tonumber(string.sub(hash, 0 , 8 ), 16)
              h[1] = tonumber(string.sub(hash, 8 , 16), 16)
              h[2] = tonumber(string.sub(hash, 16, 24), 16)
              h[3] = tonumber(string.sub(hash, 24, 32), 16)
              local maxk = math.floor(0.693147180 * math.floor((entries * math.log(precision * math.pow(0.5, index))) / -0.480453013) / entries)
              local b    = { }
              for i=1, maxk do
                table.insert(b, h[i % 2] + i * h[2 + (((i + (i % 2)) % 4) / 2)])
              end
              for n=1, index do
                local key   = key .. ':' .. n
                local found = true
                local bits = math.floor((entries * math.log(precision * math.pow(0.5, n))) / -0.480453013)
                local k = math.floor(0.693147180 * bits / entries)

                for i=1, k do
                  if redis.call('GETBIT', key, b[i] % bits) == 0 then
                    found = false
                    break
                  end
                end

                if found then
                  return 1
                end
              end

              return 0
            end

            functions.set = function(key, entries, precision, data, set_value)
              local index     = math.ceil(redis.call('INCR', key .. ':count') / entries)
              local key       = key .. ':' .. index
              local bits = math.floor(-(entries * math.log(precision * math.pow(0.5, index))) / 0.480453013)
              local k = math.floor(0.693147180 * bits / entries)
              local hash = redis.sha1hex(data)
              local h = { }
              h[0] = tonumber(string.sub(hash, 0 , 8 ), 16)
              h[1] = tonumber(string.sub(hash, 8 , 16), 16)
              h[2] = tonumber(string.sub(hash, 16, 24), 16)
              h[3] = tonumber(string.sub(hash, 24, 32), 16)
              for i=1, k do
                redis.call('SETBIT', key, (h[i % 2] + i * h[2 + (((i + (i % 2)) % 4) / 2)]) % bits, set_value)
              end
              return 1
            end

            functions.setnx = function(key, entries, precision, data, set_value)
              if functions.check(key, entries, precision, data) ~= 1 then
                functions.set(key, entries, precision, data, set_value)
                return 1
              else
                return 0
              end
            end

            local function invoke(f, ...)
              return functions[f](KEYS[1], ...)
            end

            return invoke(unpack(ARGV))
          )

          @functions_sha   = Digest::SHA1.hexdigest(functions)

          loaded = @redis.script(:exists, [@functions_sha]).uniq
          if loaded.count != 1 || loaded.first != true
            @functions_sha   = @redis.script(:load, functions)
          end
        end

        def set(data, val)
          call "set", [data, val]
        end

        def setnx(data, val)
          call "setnx", [data, val]
        end

        def call(func, args)
          @redis.evalsha(@functions_sha, :keys => [@options[:key_name]], :argv => [func, @options[:size], @options[:error_rate]] + Array(args))
        end
    end
  end
end