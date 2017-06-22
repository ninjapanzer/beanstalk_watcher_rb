module Job
  class ListQueryWorker
    require 'csv'

    def self.perform(query_id)
      query = ListQuery.find(query_id)
      input_path = query.query_data.path
      user = query.user
      client = user.client

      phone_numbers = []
      puts input_path
      CSV.foreach(input_path) do |row|
        phone_numbers << row.first
      end

      query_results = ClientPhoneNumber
        .joins(:phone_number)
        .where(client_id: client.id)
        .where(:'phone_numbers.callable_number' => phone_numbers)
        .pluck('phone_numbers.callable_number')

      # NOT OK
      # NOT IN LIST
      # UNVERIFIABLE
      phone_number_map = {}

      phone_numbers.flatten.each do |num|
        phone_number_map[num] = {
          number: num,
          status: query_results.include?(num.to_s) ? 'NOT OK' : 'UNKNOWN'
        }
      end

      result = CSV.generate do |csv|
        phone_number_map.each do |num, meta|
          csv << [num, meta[:status]]
        end
      end

      ListQueryResult.create list_query_id: query_id,
        serializer_class: 'CSV',
        result: result,
        user_id: user.id

      query.update_attribute :status, ListQuery::OK
    end

    def self.queue_priority
      100
    end

    def self.queue_respond_timeout
      2.hours.to_i
    end
  end
end
