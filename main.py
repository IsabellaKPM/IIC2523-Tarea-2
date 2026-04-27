import sys
import typing, os, pathlib, math, re, collections

from utils import DB, Bully, Logger, Node, Event

class Main:
    def __init__(self, test_path: str) -> None:
        self.test_path = test_path
        self.logs_path = self.get_logs_file_path()

        self.node_names = []
        self.n_proposing_nodes = 0
        self.n_consensus = 0
        self.logger = Logger(self.logs_path, self)
        self.should_branch = False
        self.branches = [{
            "db": DB(),
            "bully": Bully(),
        }]

    def get_logs_file_path(self) -> str:
        test_path = self.test_path.replace("tests_publicos", "logs")
        return test_path.replace(".txt", "_LOG.txt")
    
    def run(self) -> None:
        lines = self.get_lines()
        self.setup(lines[0:3])
        events = lines[3:]
        self.handle_events(events)
        self.save_logs()

    def get_lines(self) -> typing.List[str]:
        with open(self.test_path, "r") as f:
            return f.readlines()

    def setup(self, lines: typing.List[str]) -> None:
        lines = [self.process_line(line) for line in lines]

        self.node_names = lines[1].split(";")
        node_ids = lines[2].split(";")

        n_nodes = len(self.node_names)
        self.n_proposing_nodes = int(lines[0])
        self.n_consensus = n_nodes - self.n_proposing_nodes

        self.branches[0]["bully"].set_n_proposing_nodes(self.n_proposing_nodes)

        self.create_nodes(node_ids)

    def create_nodes(self, ids: typing.List[str]) -> None:
        nodes = []
        for name, id in zip(self.node_names, ids):
            nodes.append(Node(name, int(id.strip())))
        self.branches[0]["bully"].add_nodes(nodes)

    def handle_events(self, events: typing.List[str]) -> None:
        for event_command in events:
            event_command = self.process_line(event_command)
            if event_command is None:
                continue

            event = Event(event_command, self.logger, self.n_consensus)
            try:
                event.validate(self.node_names)
            except ValueError:
                continue

            current_branches = self.branches.copy()
            for branch in current_branches:
                if event.has_branching:
                    self.add_duplicate_branch(branch)
                event.run(branch["db"], branch["bully"])

    def process_line(self, line: str) -> typing.Optional[str]:
        line = line.strip()
        if not line or line.startswith("#"):
            return None
        if "#" in line:
            comment_idx = line.index("#")
            line = line[:comment_idx].strip()
        return line

    def add_duplicate_branch(self, old_branch: dict) -> None:
        self.branches.append({
            "db": old_branch["db"].copy(),
            "bully": old_branch["bully"].copy()
        })

    def save_logs(self) -> None:
        self.logger.save()
        self.save_global_data()

    def save_global_data(self) -> None:
        global_data = self.get_global_data()
        with open(self.logs_path, "a") as f:
            f.write("BASE DE DATOS\n")
            if global_data:
                for key, values in global_data.items():
                    f.write(f"{key}={str(values)}\n")
            else:
                f.write("No hay datos\n")

    def get_global_data(self) -> dict:
        global_data = {}
        for branch in self.branches:
            branch["db"].save(global_data)
        return global_data


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python3 main.py [PATH_TO_INPUT_FILE]")
        sys.exit(1)

    PATH = sys.argv[1]
    main = Main(PATH)
    main.run()

    print(sys.argv)
