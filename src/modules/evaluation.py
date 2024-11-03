import pandas as pd
import numpy as np
import matplotlib
import seaborn as sns
from src.modules.utils import print_log
from src.config import *

import matplotlib.pyplot as plt
from matplotlib.colors import hsv_to_rgb

matplotlib.use('TkAgg')  # issue with pycharm? this is a backend configuration


def evaluate_on_trec(run_dict, trec_eval):
    '''
    This function take as an argument a dict related to a query with this structure:
        query (int): Query ID.
        q0 (str): Literal "q0".
        docid (str): Document ID.
        rank (int): Rank of document.
        score (float): Score of document.
        system (str): Tag for current run.

    :param trec_eval: trec evaluation structure
    :param run_dict: dict on a single retrieval run.
    :return:
    '''
    # Structure of QREL txt file qid, “Q0”, docid, rating
    qrel = pd.read_csv(evaluation_trec_qrel_2020_path, sep=' ',
                       names=["query", "q0", "docid", "rel"])
    qrel["q0"] = "q0"
    qrel["docid"] = qrel["docid"].astype(str)
    qrel = qrel.to_dict(orient="list")
    results = {}
    if run_dict['query'][0] in qrel['query']:
        results = trec_eval.compute(references=[qrel], predictions=[run_dict])
    return results


def create_run_dict(qid, name, docID_score):
    """
    Example of a simple dict:
        run = {
        "query": [0, 0],
        "q0": ["q0", "q0"],
        "docid": ["doc_2", "doc_1"],
        "rank": [0, 1],
        "score": [1.5, 1.2],
        "system": ["test", "test"]
        }
    :param qid:
    :param name:
    :param docID_score:
    :return:
    """
    run_dict = {}
    run_dict["query"] = []
    run_dict["q0"] = []
    run_dict["docid"] = []
    run_dict["rank"] = []
    run_dict["score"] = []
    run_dict["system"] = []
    i = 0
    for doc_id, score in docID_score:
        run_dict["query"].append(int(qid))
        run_dict["q0"].append("q0")
        run_dict["docid"].append(doc_id)
        run_dict["rank"].append(i)
        run_dict["score"].append(score)
        run_dict["system"].append(name)
        i += 1
    return run_dict


def get_distinct_color(index):
    """
    Generates a distinct color for the given index.

    Parameters:
        index (int): The index for which to generate a color.

    Returns:
        tuple: A (r, g, b) tuple representing the color in RGB format.
    """
    # Base number of different hue divisions
    base_hue_divisions = 12  # Number of unique hues before starting to adjust saturation or value

    # Compute hue by cycling around the HSV color wheel
    hue = (index % base_hue_divisions) / base_hue_divisions  # evenly spaced hue values

    # Increase saturation and value slightly for each round after base_hue_divisions
    saturation = 0.9 - 0.05 * (index // base_hue_divisions)  # decrease saturation slightly after each round
    value = 0.9 - 0.05 * (index // (2 * base_hue_divisions))  # decrease brightness after every two rounds

    # Ensure saturation and value stay within valid [0, 1] range
    saturation = max(0.5, saturation)
    value = max(0.5, value)

    # Create the HSV color and convert to RGB
    hsv_color = np.array([hue, saturation, value])
    rgb_color = hsv_to_rgb(hsv_color)

    return rgb_color


def plot_metrics_line_charts(data_list):
    '''
    METRICS offered by huggingfaces
    runid (str): Run name.
    num_ret (int): Number of retrieved documents.
    num_rel (int): Number of relevant documents.
    num_rel_ret (int): Number of retrieved relevant documents.
    num_q (int): Number of queries.
    map (float): Mean average precision.
    gm_map (float): geometric mean average precision.
    bpref (float): binary preference score.
    Rprec (float): precision@R, where R is number of relevant documents.
    recip_rank (float): reciprocal rank
    P@k (float): precision@k (k in [5, 10, 15, 20, 30, 100, 200, 500, 1000]).
    NDCG@k (float): nDCG@k (k in [5, 10, 15, 20, 30, 100, 200, 500, 1000]).
    '''
    metrics = ['map', 'P@30', 'Rprec', 'NDCG@5']

    grouped_data = {}
    all_qids = []

    for item in data_list:
        qid = int(item['qid'])
        if qid not in all_qids:
            all_qids.append(qid)
        name = item['name']

        # check if examining a new index
        if name not in grouped_data.keys():
            # if new, initialize a new set of lists
            # list of qids (x axis)
            grouped_data[name] = {'qid': [], 'exec_time_s': []}
            for metric in metrics:
                # list of the values for each metric (grouped by index name)
                grouped_data[name][metric] = []

        # query id and execution times
        grouped_data[name]['qid'].append(qid)
        grouped_data[name]['exec_time_s'].append(item['exec_time_s'])

        # metric values
        for metric in metrics:
            # some values might be missing if the query result is empty
            if metric in item.keys():
                # data is present
                grouped_data[name][metric].append(item[metric])
            else:
                # blank
                grouped_data[name][metric].append(0)

    all_qids = sorted(all_qids)

    # PLOT ISSUE: if query ids are not ordered, figures are weird shapes.
    # to ensure they're not overlapping lines, enforce query ids to be ordered
    for name in grouped_data:
        # Extract the 'qid' list and the other lists corresponding to the metrics
        qids = grouped_data[name]['qid']

        # list of where to move each value
        sorted_indices = sorted(range(len(qids)), key=lambda i: qids[i])

        # Sort the qid list based on the sorted indices
        grouped_data[name]['qid'] = [grouped_data[name]['qid'][i] for i in sorted_indices]

        # Sort all other lists (exec_time_s, map, etc.) based on the sorted qid order
        for key in grouped_data[name]:
            if key != 'qid':  # Skip 'qid' because it is already sorted
                grouped_data[name][key] = [grouped_data[name][key][i] for i in sorted_indices]

    print_log("plotting metrics", 3)
    fig, axs = plt.subplots(2, 5)
    axs[0, 0].set_title(metrics[0])
    axs[1, 0].set_title(metrics[0])
    axs[0, 1].set_title(metrics[1])
    axs[1, 1].set_title(metrics[1])
    axs[0, 2].set_title(metrics[2])
    axs[1, 2].set_title(metrics[2])
    axs[0, 3].set_title(metrics[3])
    axs[1, 3].set_title(metrics[3])
    axs[0, 4].set_title("execution time")
    axs[1, 4].set_title("execution time")

    group_names = []
    heatmaps = [np.zeros((len(grouped_data), len(all_qids)))]
    for _ in range(len(metrics)):
        heatmaps.append(np.zeros((len(grouped_data), len(all_qids))))
    for index_num, (index_name, index_graph) in enumerate(grouped_data.items()):
        color = get_distinct_color(index_num)
        # qids (x values) are treated like categorical flags, and not integers (there is no correlation between qids)
        x = []
        for local_x in index_graph['qid']:
            x.append(all_qids.index(local_x))
        name = index_name.replace("_", " ")

        # lines charts
        axs[0, 0].plot(x, index_graph[metrics[0]], color=color, label=name)
        axs[0, 1].plot(x, index_graph[metrics[1]], color=color)
        axs[0, 2].plot(x, index_graph[metrics[2]], color=color)
        axs[0, 3].plot(x, index_graph[metrics[3]], color=color)
        axs[0, 4].plot(x, index_graph["exec_time_s"], color=color)

        # heatmaps for metrics
        group_names.append(name)  # Add group name to the list

        for plot, metric in enumerate(metrics):
            for qid_idx, qid in enumerate(all_qids):
                if qid in index_graph['qid']:  # If qid exists in this group's data
                    value_idx = index_graph['qid'].index(qid)
                    heatmaps[plot][index_num][qid_idx] = index_graph[metric][value_idx]  # Assign the metric value
                else:
                    heatmaps[plot][index_num][qid_idx] = np.nan  # If qid is missing, fill with NaN (no color)

        # heatmap for execution time
        for qid_idx, qid in enumerate(all_qids):
            if qid in index_graph['qid']:  # If qid exists in this group's data
                value_idx = index_graph['qid'].index(qid)
                heatmaps[-1][index_num][qid_idx] = index_graph["exec_time_s"][value_idx]
            else:
                heatmaps[-1][index_num][qid_idx] = np.nan  # If qid is missing, fill with NaN (no color)

        # index_num += 1
    for i, heatmap_data in enumerate(heatmaps):
        sns.heatmap(heatmap_data, ax=axs[1, i], cmap="YlOrBr", annot=True, fmt=".1f",
                    xticklabels=all_qids, yticklabels=group_names if i == 0 else False)

    # Set x-axis labels for other plots if needed
    for ax in axs.flat:
        ax.set_xticks(range(len(all_qids)))  # Set the x-ticks to the categories
        ax.set_xticklabels(all_qids, rotation=80)  # Rotate for better visibility

    fig.legend(loc='upper right')
    plt.tight_layout()
    # Display the chart
    plt.show()


def read_query_file(file_pointer):
    line = file_pointer.readline()
    if line == "":
        return -1, ""
    content = line.split("\t")
    query_id = content[0]
    query_string = " ".join(content[1:])
    return query_id, query_string


def make_name(query_handler, search_algorithm):
    name = query_handler.index.name
    name += " " + query_handler.index.scoring
    name += " " + str(query_handler.index.topk)
    name += " " + query_handler.index.algorithm  # conjunctive or disjunctive
    name += " " + search_algorithm  # ternary or skipping
    return name
