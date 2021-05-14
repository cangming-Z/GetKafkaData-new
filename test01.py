class Structure:
    def __init__(self, adr, topic, tag):
        self.kafka_ip = adr
        self.kafka_topic = topic
        self.tag_detail = tag


class TagDetail:
    def __init__(self, tag_name, enable, date_type, value_type, limit, upper, step, reload):
        self.tag_name = tag_name
        self.enable = enable
        self.date_type = date_type
        self.value_type = value_type
        self.limit = limit
        self.upper = upper
        self.step = step
        self.reload = reload
