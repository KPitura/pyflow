from .models import Workflow, WorkflowValidationError

def _build_graph(workflow: Workflow):
    adj = {name: [] for name in workflow.tasks}
    in_degree = {name: 0 for name in workflow.tasks}
    
    for name, task in workflow.tasks.items():
        in_degree[name] = len(task.dependencies)
        for dep in task.dependencies:
            if dep in adj:
                adj[dep].append(name)
    return adj, in_degree

def topological_order(workflow: Workflow) -> list[str]:
    adj, in_degree = _build_graph(workflow)

    queue = [name for name, deg in in_degree.items() if deg == 0]
    queue.sort()
    
    result = []
    
    while queue:
        curr = queue.pop(0)
        result.append(curr)
        
        next_nodes = []
        for neighbor in adj[curr]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                next_nodes.append(neighbor)

        if next_nodes:
            next_nodes.sort()
            queue.extend(next_nodes)
            queue.sort()
            
    if len(result) != len(workflow.tasks):
        raise WorkflowValidationError("Wykryto cykl w przepływie (DAG).")
        
    return result

def execution_layers(workflow: Workflow) -> list[list[str]]:

    adj, in_degree = _build_graph(workflow)
    
    current_layer = [name for name, deg in in_degree.items() if deg == 0]
    current_layer.sort()
    
    layers = []
    processed_count = 0
    
    while current_layer:
        layers.append(current_layer)
        processed_count += len(current_layer)
        
        next_layer = []
        for node in current_layer:
            for neighbor in adj[node]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    next_layer.append(neighbor)
        
        next_layer.sort()
        current_layer = next_layer
        
    if processed_count != len(workflow.tasks):
        raise WorkflowValidationError("Wykryto cykl w przepływie (DAG).")
        
    return layers