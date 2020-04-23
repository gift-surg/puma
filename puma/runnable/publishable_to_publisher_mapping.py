from typing import Dict

from puma.buffer import Publisher
from puma.runnable._in_runnable_indirect_publisher import _InRunnableIndirectPublisher


class PublishableToPublisherMapping(Dict[int, _InRunnableIndirectPublisher]):

    def cross_scope_accessor(self, pub_id: int) -> Publisher:
        publisher = self.get(pub_id, None)
        if not publisher:
            raise RuntimeError("Publisher has not been set on _InRunnableIndirectPublisher. Was this called before running run_execute()?")
        return publisher
