require "bunny"
require "json"
require "thread"

class Replay

  REPLAY_EXCHANGE = "replay".freeze
  MAX_RETRIES     = 5
  PREFETCH        = ENV.fetch("PLUMBING_EVENT_REPLAY_PREFETCH", 250).to_i

  SUBSCRIBE_OPTIONS = {
    manual_ack: true,
    exclusive:  true
  }.freeze

  def initialize(queue_name)
    @queue_name = queue_name
  end

  def run
    conn = Bunny.new.start

    chan = conn.create_channel
    chan.prefetch(PREFETCH)

    queue = chan.queue(@queue_name)
    exch  = chan.direct(REPLAY_EXCHANGE, durable: true)

    queue.bind(exch, routing_key: @queue_name)

    replay_events_loop(chan, queue)

    queue.delete
    conn.close
  end


  def replay_events_loop(channel, queue)
    semaphore   = Mutex.new
    replay_done = false

    queue.subscribe(SUBSCRIBE_OPTIONS) do |delivery_info, _, payload|
      begin
        next_loop_state = process_payload(JSON.parse(payload))

        channel.ack(delivery_info.delivery_tag)
        if next_loop_state == :stop
          channel.consumers[delivery_info.consumer_tag].cancel
          semaphore.synchronize { replay_done = true }
        end
      rescue => exception
        channel.reject(delivery_info.delivery_tag, true)
        exit(1)
      end
    end

    # Run forever until next_loop_state == :stop.
    loop do
      sleep 0.5
      semaphore.synchronize do
        puts "Replay done? #{replay_done}"
        if replay_done
          puts "Finished replaying events"
          return
        end
      end
    end

  end

  def process_payload(payload)
    command_name = payload.fetch("meta", {}).fetch("command", nil)

    if command_name
      process_command(command_name, payload.fetch("data"))
    else
      process_event(payload)
    end
  end

  def process_command(name, data)
    case name
      when "replay_complete"
        :stop
    end
  end

  def process_event(payload)
    :continue
  end

end
