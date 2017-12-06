require "bunny"
require "json"
require "thread"

class Replay

  SUBSCRIBE_OPTIONS = {
    manual_ack: true,
    exclusive:  true
  }.freeze

  def initialize(queue_name)
    @queue_name = queue_name
  end

  def run
    conn  = Bunny.new.start
    chan  = conn.create_channel
    queue = chan.queue(@queue_name)
    exch  = chan.direct("replay", durable: true)

    queue.bind(exch, routing_key: @queue_name)

    replay_events_loop(chan, queue)

    queue.delete
    conn.close
  end

  private

  def replay_events_loop(channel, queue)
    replay_done = false

    consumer = queue.subscribe(SUBSCRIBE_OPTIONS) do |delivery_info, _, payload|
      begin
        next_loop_state = process_payload(JSON.parse(payload))

        channel.ack(delivery_info.delivery_tag)
        if next_loop_state == :stop
          replay_done = true
        end
      rescue => exception
        channel.reject(delivery_info.delivery_tag, true)
        exit(1)
      end
    end

    # Run forever until next_loop_state == :stop.
    loop do
      sleep 2
        if replay_done
          consumer.cancel
          puts "Finished replaying events"
          return
      end
    end

  end

  def process_payload(payload)
    payload.fetch("meta", {}).fetch("command", nil) ? :stop : :continue
  end

end
