"""kvcli is a tiny command line client.

    python3 -m kvstore.cmd.kvcli [-addr host:port] put KEY VALUE | get KEY | del KEY | range START END | batch K1 V1 K2 V2 ...
    (use "" for an open range bound)
"""

import argparse
import sys

from .. import kvnet


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="kvcli", description="tiny kvstore client")
    ap.add_argument("-addr", "--addr", default="127.0.0.1:7000",
                    help="server address, or comma-separated list for a cluster")
    ap.add_argument("command", nargs="*")
    args = ap.parse_args(argv)
    a = args.command
    if not a:
        print("usage: kvcli put|get|del|range|batch ...", file=sys.stderr)
        return 2

    def bound(s: str):
        return s.encode() if s else None

    try:
        if "," in args.addr:
            c = kvnet.dial_cluster(args.addr.split(","), 10.0, 15.0)
        else:
            c = kvnet.dial(args.addr, 10.0)
    except OSError as e:
        print("error:", e, file=sys.stderr)
        return 1
    try:
        if a[0] == "put" and len(a) == 3:
            c.put(a[1].encode(), a[2].encode())
        elif a[0] == "del" and len(a) == 2:
            c.delete(a[1].encode())
        elif a[0] == "get" and len(a) == 2:
            sys.stdout.buffer.write(c.read(a[1].encode()) + b"\n")
            sys.stdout.buffer.flush()
        elif a[0] == "range" and len(a) == 3:
            for kv in c.read_key_range(bound(a[1]), bound(a[2])):
                sys.stdout.buffer.write(kv.key + b"\t" + kv.value + b"\n")
            sys.stdout.buffer.flush()
        elif a[0] == "batch" and len(a) >= 3 and len(a) % 2 == 1:
            ks = [a[i].encode() for i in range(1, len(a), 2)]
            vs = [a[i].encode() for i in range(2, len(a), 2)]
            c.batch_put(ks, vs)
        else:
            print("bad arguments", file=sys.stderr)
            return 2
    except Exception as e:
        print("error:", e, file=sys.stderr)
        return 1
    finally:
        c.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
