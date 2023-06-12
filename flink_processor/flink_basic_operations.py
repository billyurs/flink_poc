
import json
import logging
import sys

from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment


def show(ds, env):
    ds.print()
    env.execute()


def basic_operations():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # define the source
    ds = env.from_collection(
        collection=[
            (1, '{"name": "Flink", "tel": 123, "addr": {"country": "Germany", "city": "Berlin"}}'),
            (2, '{"name": "hello", "tel": 135, "addr": {"country": "China", "city": "Shanghai"}}'),
            (3, '{"name": "world", "tel": 124, "addr": {"country": "USA", "city": "NewYork"}}'),
            (4, '{"name": "PyFlink", "tel": 32, "addr": {"country": "China", "city": "Hangzhou"}}')
        ],
        type_info=Types.ROW_NAMED(["id", "info"], [Types.INT(), Types.STRING()])
    )

    # map
    def update_tel(data):
        # parse the json
        json_data = json.loads(data.info)
        json_data['tel'] += 1
        return data.id, json.dumps(json_data)

    show(ds.map(update_tel), env)
    # (1, '{"name": "Flink", "tel": 124, "addr": {"country": "Germany", "city": "Berlin"}}')
    # (2, '{"name": "hello", "tel": 136, "addr": {"country": "China", "city": "Shanghai"}}')
    # (3, '{"name": "world", "tel": 125, "addr": {"country": "USA", "city": "NewYork"}}')
    # (4, '{"name": "PyFlink", "tel": 33, "addr": {"country": "China", "city": "Hangzhou"}}')

    # filter
    show(ds.filter(lambda data: data.id == 1).map(update_tel), env)
    # (1, '{"name": "Flink", "tel": 124, "addr": {"country": "Germany", "city": "Berlin"}}')

    # key by
    show(ds.map(lambda data: (json.loads(data.info)['addr']['country'],
                              json.loads(data.info)['tel']))
           .key_by(lambda data: data[0]).sum(1), env)
    # ('Germany', 123)
    # ('China', 135)
    # ('USA', 124)
    # ('China', 167)


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    basic_operations()