# MCAP Writer for MicroPython

This repository contains a [MicroPython][]-compatible fork of the `mcap` Python package for writing [MCAP][] container files.

  [MCAP]: https://mcap.dev/
  [MicroPython]: https://micropython.org/

This repository is based on release 1.1.0 of the MCAP Python package. 


## API Reference

Please refer to the [`mcap` API Reference][api].

  [api]: https://mcap.dev/docs/python/


## Differences from the Python module

  * Only writing is supported.
  * Compression is not supported.
  * I/O is unbuffered.


## Example

```
$ micropython -m mip install github:rgov/micropython-mcap
$ micropython examples/raw/writer.py test.mcap
```
