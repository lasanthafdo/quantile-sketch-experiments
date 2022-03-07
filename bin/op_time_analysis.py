import sys

import matplotlib.pyplot as plt
import pandas as pd
from itertools import cycle


def produce_imq_bar_plot(data_df, plot_title, x_axis, x_label, y_label, plot_filename, y_limit=0.0):
    ax = data_df.plot(x=x_axis, y=["Moments", "DDS", "UDDS", "KLL", "REQ"], kind="bar")
    if y_limit > 0:
        for p in ax.patches:
            if p.get_height() > y_limit:
                ax.annotate(str(p.get_height()), (p.get_x() + 0.03, 4 * y_limit / 5), rotation=90)
        plt.ylim(0, y_limit)

    if x_axis == "Quantile":
        ax.set_xticklabels(["Other quantiles", "50th quantile"])
    plt.xticks(rotation=0)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.title(plot_title)
    # plt.legend(loc="lower right")
    plt.savefig(plot_filename)
    plt.clf()
    print("Finished generating plots.")
    # plt.show()
    plt.clf()


def produce_imq_line_plot(data_df, plot_title, x_axis, x_label, y_label, plot_filename):
    ax = data_df.plot(x=x_axis, y=["Moments", "DDS", "UDDS", "KLL", "REQ"], style=['o-', 'v-', '^-', '|--', 'x--'])
    if x_axis == "Kurtosis":
        ax.set_xscale('symlog')
        plt.xlim(-2, 1000000)
        plt.ylim(0, 0.08)
    elif x_axis == "Data Size":
        ax.set_xscale('log')

    # plt.xticks(rotation=0)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.title(plot_title)
    # plt.legend(loc="lower right")
    plt.savefig(plot_filename)
    plt.clf()
    print("Finished generating plots.")
    # plt.show()
    plt.clf()


def produce_imq_scatter_plot(data_df, plot_title, x_axis, x_label, y_label, plot_filename):

    size = 20
    fig, ax = plt.subplots()
    for i, alg in enumerate(["DDS","UDDS","KLL","REQ","Moments"]):
        offset = 0.7 + i * 0.2
        ax.scatter(data_df[x_axis] * offset, data_df[alg], s=size, label=alg)

    ax.legend()

    if x_axis == "Kurtosis":
        plt.xscale('symlog')
        plt.xlim(-2, 1000000)
        plt.ylim(0, 0.08)
    elif x_axis == "Data Size":
        plt.xscale('log')

    # plt.xticks(rotation=0)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.title(plot_title)
    # plt.legend()
    plt.savefig(plot_filename)
    plt.clf()
    print("Finished generating plots.")
    # plt.show()
    plt.clf()


if __name__ == '__main__':
    report_folder = sys.argv[1]

    f_query_times = report_folder + '/query_times.csv'
    query_times_df = pd.read_csv(f_query_times)
    print(query_times_df)
    q_plot_file_name = report_folder + '/plots/query_times.pdf'
    produce_imq_line_plot(query_times_df, 'Query time', "Data Size", 'Data size', 'Time (microseconds)',
                          q_plot_file_name)

    f_insert_times = report_folder + '/insert_times.csv'
    insert_times_df = pd.read_csv(f_insert_times)
    insert_times_df.iloc[:, 1:] = insert_times_df.iloc[:, 1:].div(insert_times_df['Data Size'], axis=0)
    insert_times_df['Data Size'] = insert_times_df['Data Size'] // 1000000
    print(insert_times_df)
    i_plot_file_name = report_folder + '/plots/insert_times.pdf'
    produce_imq_bar_plot(insert_times_df, 'Insertion time for each data point', "Data Size", 'Data size (millions)',
                         'Time (microseconds)', i_plot_file_name)

    f_merge_times = report_folder + '/merge_times.csv'
    merge_times_df = pd.read_csv(f_merge_times)
    print(merge_times_df)
    i_plot_file_name = report_folder + '/plots/merge_times.pdf'
    produce_imq_bar_plot(merge_times_df, 'Merge time', "Num Sketches", 'Number of sketches', 'Time (microseconds)',
                         i_plot_file_name, 25000)

    f_kurtosis = report_folder + '/kurtosis.csv'
    kurtosis_df = pd.read_csv(f_kurtosis)
    print(kurtosis_df)
    kurtosis_df.iloc[:, 3:] = kurtosis_df.iloc[:, 3:].sub(kurtosis_df['Actual'], axis=0).div(
        kurtosis_df['Actual'], axis=0).abs().round(4)
    kurtosis_df = kurtosis_df.sort_values(by=['Kurtosis'])
    print(kurtosis_df)
    k_plot_file_name = report_folder + '/plots/kurtosis.pdf'
    produce_imq_line_plot(kurtosis_df, 'Kurtosis vs Accuracy', "Kurtosis", 'Kurtosis',
                          'Avg. Relative Error (98th percentile)', k_plot_file_name)

    f_adaptability = report_folder + '/adaptability.csv'
    adaptability_df = pd.read_csv(f_adaptability)
    adaptability_df.iloc[:, 2:] = adaptability_df.iloc[:, 2:].sub(adaptability_df['Actual'], axis=0).div(
        adaptability_df['Actual'], axis=0).abs().round(4)
    print(adaptability_df)
    adaptability_df.loc[adaptability_df['Quantile'] != 0.5, 'Quantile'] = 0
    adaptability_df.loc[adaptability_df['Quantile'] == 0.5, 'Quantile'] = 1
    adaptability_df = adaptability_df.groupby('Quantile', as_index=False).mean().round(4)
    print(adaptability_df)
    a_plot_file_name = report_folder + '/plots/adaptability.pdf'
    produce_imq_bar_plot(adaptability_df, 'Adaptability', "Quantile", 'Quantiles',
                         'Avg. Relative Error', a_plot_file_name, 0.08)

    f_query_time_tests = report_folder + '/query_time_tests.csv'
    query_time_tests_df = pd.read_csv(f_query_time_tests)
    print(query_time_tests_df)
    q_plot_file_name = report_folder + '/plots/query_time_tests.png'
    produce_imq_scatter_plot(query_time_tests_df, 'Query time', "Data Size", 'Data size', 'Time (microseconds)',
                             q_plot_file_name)
