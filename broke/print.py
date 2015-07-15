import broke
import sys
import argparse

def parse_arguments():
    parser = argparse.ArgumentParser(description="Prints information about a broke-file")
    parser.add_argument("--verbose", action="store_true", help="Print each message")
    parser.add_argument("--utf8", action="store_true", help="Prints payload of each message decoded as utf8")
    parser.add_argument("--no-count", action="store_true", help="Do not print the total number of messages at the end")
    parser.add_argument("--follow", action="store_true", help="Follows the stream of messages")
    parser.add_argument("--topic", type=str, default=None, help="Only read messages of this topic")
    parser.add_argument("file", help="The file to read messaages from")
    return parser.parse_args()


def main():
    args = parse_arguments()

    read_messages = broke.read_messages_follow if args.follow else broke.read_messages

    count = 0
    with open(args.file, "rb") as fp:
        try:
            for message in read_messages(fp):
                if args.topic is not None and message.topic != args.topic:
                    continue

                count += 1

                if args.verbose:
                    print(message)

                if args.utf8:
                    print(message.payload.decode("utf8"))

        except KeyboardInterrupt:
            pass

        if not args.no_count:
            print("Total number of messages: {}".format(count))


if __name__ == '__main__':
    main()
