from pyflink.common import Encoder
from pyflink.datastream.connectors import BucketAssigner
from pyflink.java_gateway import get_gateway


class JsonEncoder(Encoder):
    def __init__(self, charset_name: str = "UTF-8"):
        j_encoder = get_gateway().jvm.org.cdaps.JsonEncoder(charset_name)
        super().__init__(j_encoder)


class KeyBucketAssigner(BucketAssigner):
    def __init__(self, key: str):
        super().__init__(get_gateway().jvm.org.cdaps.KeyBucketAssigner(key))
