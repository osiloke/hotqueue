=============
API Reference
=============

.. autofunction:: hotqueue.key_for_name

.. autoclass:: hotqueue.HotQueue(name, serializer=pickle, **kwargs)

    .. autoattribute:: hotqueue.HotQueue.key

    .. automethod:: hotqueue.HotQueue.clear

    .. automethod:: hotqueue.HotQueue.consume

    .. automethod:: hotqueue.HotQueue.get

    .. automethod:: hotqueue.HotQueue.put
    
    .. automethod:: hotqueue.HotQueue.worker
