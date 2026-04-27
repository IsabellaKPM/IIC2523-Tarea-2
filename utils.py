import re
from typing import Dict, List

# Pseudo-Enum for event commands
PREPARE = "Prepare"
ACCEPT = "Accept"
LEARN = "Learn"
STOP = "Stop"
START = "Start"


class DB:
    def __init__(self) -> None:
        self.data = {}

    def run_command(self, command: str) -> None:
        command = command.split("-", 2)
        command = [part.strip() for part in command]
        command_type = command[0]
        command_args = command[1:]
        if command_type == "SET":
            self.set(*command_args)
        elif command_type == "ADD":
            self.add(*command_args)
        elif command_type == "DEL":
            self.delete(*command_args)

    def get(self, key: str) -> str:
        value = self.data.get(key, "None")
        print(f"GET {key} -> {value}")
        return str(value)
    
    def set(self, key: str, value: str) -> None:
        if not self.is_params_valid(key, value):
            return
        if value.isdigit():
            value = int(value)
        print(f"SET {key}={value}")
        self.data[key] = value

    def add(self, key: str, value: str) -> None:
        if key not in self.data:
            self.set(key, value)
            return
        if not self.is_params_valid(key, value):
            return

        current = self.data[key]
        if value.isdigit() and str(current).isdigit():
            self.data[key] = int(current) + int(value)
        else:
            self.data[key] = str(current) + value

    def is_params_valid(self, key: str, value: str) -> bool:
        return self.is_key_name_valid(key) and self.is_value_valid(value)

    def is_key_name_valid(self, key: str) -> bool:
        return re.match(r"^[a-zA-Z _]+$", key) is not None

    def is_value_valid(self, value: str) -> bool:
        if value.isdigit():
            if int(value) < 0:
                return False
        return True
    
    def delete(self, key: str) -> None:
        if key in self.data:
            self.data.pop(key)

    def copy(self) -> "DB":
        new_db = DB()
        new_db.data = self.data.copy()
        return new_db

    def save(self, global_data: Dict) -> Dict:
        for key, value in self.data.items():
            if key not in global_data:
                global_data[key] = [value]
            elif value not in global_data[key]:
                global_data[key].append(value)
        return global_data


class Logger:
    def __init__(self, path: str, main: 'Main') -> None:
        self.path = path
        self.logs = set()
        self.main = main

    def log(self, variable: str) -> None:
        global_data = self.main.get_global_data()
        self.logs.add(f"{variable}={str(global_data.get(variable, []))}")
    
    def save(self) -> None:
        with open(self.path, "w") as f:
            f.write("LOGS\n")
            if len(self.logs) == 0:
                f.write("No hubo logs\n")
            else:
                f.write("\n".join(self.logs))
                f.write("\n")


class Node:
    def __init__(self, name: str, id: int) -> None:
        self.name = name
        self.id = id
        self.is_up = True
        self.is_proposer = False

        # Acceptor
        self.promise_id = None
        self.accepted_id = None
        self.accepted_value = None

        # Proposer
        self.last_prepare_id = None
        self.promises_received  = []

    def start(self) -> None:
        self.is_up = True

    def stop(self) -> None:
        self.is_up = False

    def make_proposer(self) -> None:
        if not self.is_proposer:
            self.reset()
        self.is_proposer = True

    def make_acceptor(self) -> None:
        if self.is_proposer:
            self.reset()
        self.is_proposer = False

    def reset(self) -> None:
        self.promise_id = None
        self.accepted_id = None
        self.accepted_value = None
        self.last_prepare_id = None
        self.promises_received  = []

    def copy(self) -> "Node":
        new_node = Node(self.name, self.id)
        new_node.is_up = self.is_up
        new_node.is_proposer = self.is_proposer
        new_node.promise_id = self.promise_id
        new_node.accepted_id = self.accepted_id
        new_node.accepted_value = self.accepted_value
        new_node.last_prepare_id = self.last_prepare_id
        new_node.promises_received  = self.promises_received .copy()
        return new_node


class Bully:
    def __init__(self) -> None:
        self.nodes = []
        self.n_proposing_nodes = 0

    def set_n_proposing_nodes(self, n: int) -> None:
        self.n_proposing_nodes = n

    def add_nodes(self, nodes: List[Node]) -> None:
        self.nodes = sorted(nodes, key=lambda node: node.id, reverse=True)
        self.set_proposers()

    def set_proposers(self) -> Node:
        available_nodes = self.get_available_nodes()
        for node in available_nodes:
            if self.nodes.index(node) < self.n_proposing_nodes:
                node.make_proposer()
            else:
                node.make_acceptor()

    def get_nodes_by_rol(self, is_proposer: bool) -> List[Node]:
        return [node for node in self.get_available_nodes() if node.is_proposer == is_proposer]

    def get_available_nodes(self) -> List[Node]:
        return [node for node in self.nodes if node.is_up]

    def get_available_nodes_by_rol(self, is_proposer: bool) -> List[Node]:
        return [node for node in self.get_available_nodes() if node.is_proposer == is_proposer and node.is_up]

    def get_node_by_name(self, name: str) -> Node:
        for node in self.nodes:
            if node.name == name:
                return node

    def copy(self) -> "Bully":
        new_bully = Bully()
        new_bully.n_proposing_nodes = self.n_proposing_nodes
        new_bully.nodes = [node.copy() for node in self.nodes]
        return new_bully


class Event:
    def __init__(self, line: str, logger: Logger, n_consensus: int) -> None:
        self.line = line
        self.logger = logger
        self.n_expected_consensus = n_consensus
        self.has_branching = False
        self.setup()

    def setup(self) -> None:
        params = self.line.split(";")
        params = [param.strip() for param in params]
        self.command = params[0]
        if self.command.startswith("*"):
            self.command = self.command[1:]
            self.has_branching = True
        self.args = params[1:]

    def validate(self, existing_node_names: List[str]) -> None:
        if self.command in {STOP, START}:
            self.validate_nodes(existing_node_names, self.args)
        elif self.command in {PREPARE, ACCEPT}:
            self.validate_nodes(existing_node_names, [self.args[0]])

    def validate_nodes(self, existing_node_names: List[str], node_names: List[str]) -> None:
        for name in node_names:
            if name not in existing_node_names:
                raise ValueError("INVALID NODE")

    def run(self, db: DB, bully: Bully) -> None:
        self.db = db
        self.bully = bully
        if self.command == PREPARE:
            self.prepare()
        elif self.command == ACCEPT:
            self.accept()
        elif self.command == STOP:
            self.stop()
        elif self.command == START:
            self.start()
        elif self.command == LEARN:
            self.learn()
        else:
            self.log()

    def prepare(self) -> None:
        node_name = self.args[0]
        proposal_id = int(self.args[1])

        proposer = self.bully.get_node_by_name(node_name)
        proposer.last_prepare_id = proposal_id
        proposer.promises_received = []

        available_acceptors = self.bully.get_available_nodes_by_rol(False)
        for acceptor in available_acceptors:
            if acceptor.promise_id is None or proposal_id > acceptor.promise_id:
                acceptor.promise_id = proposal_id
                response = {
                    "accepted_id": acceptor.accepted_id,
                    "accepted_value": acceptor.accepted_value
                }
                proposer.promises_received.append(response)

    def accept(self) -> None:
        node_name = self.args[0]
        proposal_id = int(self.args[1])
        db_command = self.args[2]

        proposer = self.bully.get_node_by_name(node_name)
        if proposer.last_prepare_id != proposal_id:
            return
        if len(proposer.promises_received) < self.get_n_consensus():
            return

        highest_accepted_id = -1
        accepted_value = None
        for promise in proposer.promises_received:
            if promise["accepted_id"] is not None and promise["accepted_id"] > highest_accepted_id:
                highest_accepted_id = promise["accepted_id"]
                accepted_value = promise["accepted_value"]
                
        if accepted_value is None:
            accepted_value = db_command

        available_acceptors = self.bully.get_available_nodes_by_rol(False)
        for acceptor in available_acceptors:
            if acceptor.promise_id is None or proposal_id >= acceptor.promise_id:
                acceptor.promise_id = proposal_id
                acceptor.accepted_id = proposal_id
                acceptor.accepted_value = accepted_value


    def stop(self) -> None:
        node_names = self.args
        for node in self.bully.nodes:
            if node.name in node_names:
                node.stop()
                if node.is_proposer:
                    self.bully.set_proposers()

    def start(self) -> None:
        node_names = self.args
        for node in self.bully.nodes:
            if node.name in node_names:
                node.start()
                if node.is_proposer:
                    self.bully.set_proposers()

    def learn(self) -> None:
        available_acceptors = self.bully.get_available_nodes_by_rol(False)
        counts = {}
        for acceptor in available_acceptors:
            if acceptor.accepted_value:
                value = acceptor.accepted_value
                counts[value] = counts.get(value, 0) + 1
        if not counts:
            return

        print("COUNTS:", counts)
        maximum_value = max(counts, key=counts.get, default=None)
        max_count = counts.get(maximum_value, 0)
        if max_count >= self.get_n_consensus():
            self.db.run_command(maximum_value)

        for node in self.bully.get_available_nodes():
            node.reset()

    def log(self) -> None:
        variable = self.args[0]
        self.logger.log(variable)

    def get_n_consensus(self) -> int:
        return min(self.n_expected_consensus, len(self.bully.get_available_nodes_by_rol(False)))
