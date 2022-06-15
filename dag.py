from collections import deque

class Pipeline(object):
    def __init__(self):
        self.graph= {}

    def add(self, node, to=None):
        """
            This function adds a node(task) as key to the graph dictionary and the
            dependent(s) of that node(task) as a list.
        """
        if node not in self.graph:
            self.graph[node] = []

        if to:
            if to not in self.graph:
                self.graph[to] = []
                self.graph[node].append(to)
            else:
                self.graph[node].append(to)

        # Check that acyclic and not cyclic
        # if len(self.organize_tasks()) > len(self.graph):
        #     raise ValueError("Cyclic DAG created")

    def count_depencies(self):
        """
            This function determines nodes(tasks) that do not depend on any other task i.e root nodes(tasks) i.e they do not have dependencies
            The self.graph dictionary represents nodes and their respective dependants, so if we are to count the number of times a node appears
            in the dependants list, it will give us the number of depencies that node has and if it is zero then that is a root node(task)
        """
        self.depedencies = {}
        for node in self.graph:
            self.depedencies[node] = 0

        for node, dependants in self.graph.items():
            for dep in dependants:
                if dep in self.depedencies:
                    self.depedencies[dep] += 1

        return self.depedencies


    def organize_tasks(self):
        """
            This function will organize the node(tasks) in preparation for execution starting
            with the root nodes(tasks) followed by the dependants and so on
        """
        root_queue = deque()
        self.count_depencies()
        # when a root node(task) is found in append it to the root_queue and remove it from self.graph
        while len(self.graph) > 0:
            for node, counted_dependants in self.depedencies.items():
                if counted_dependants == 0:
                    root_queue.append(node)
                    self.graph.pop(node, None)
                    self.count_depencies() # we re run the function to find and set new root nodes.

        return root_queue

    

    def run(self):
        """
            This function will run the tasks(nodes) as organized by the organize_task function above
            It will also create a dictionary that will save the output of each task
        """
        counted_dependencies = self.count_depencies()
        completed_tasks = {}
        for task in counted_dependencies:
            for node, dependants in self.graph.items():
                if task in dependants:
                    completed_tasks[task] = task(completed_tasks[node])

            if task not in completed_tasks:
                completed_tasks[task] = task()

        return completed_tasks


    
    def task(self, depends_on=None):
        """
            This function adds pipeline tasks to the dag in preparation for execution
            It will be used for as a decorator for each of the defined pipeline tasks
        """
        def inner(func):
            self.add(func)
            if depends_on:
                self.add(depends_on, func)
            return func 
        return inner
