import sys

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def produce_imq_bar_plot(data_df, plot_title, x_axis, x_label, y_label, plot_filename, y_limit=0.0):
    if x_axis == "Quantile":
        data_df = data_df.drop('Run ID', axis=1).groupby("Quantile", as_index=False).mean()
    if plot_title == "Insertion time for each data point":
        data_df = data_df.groupby("Data Size", as_index=False).mean()
    elif plot_title == "Merge time per sketch":
        data_df = data_df.groupby("Num Sketches", as_index=False).mean()

    print(data_df)

    ax = data_df.plot(x=x_axis, y=["Moments", "DDS", "UDDS", "KLL", "REQ"], kind="bar")
    if y_limit > 0:
        plt.yscale('log')
        plt.ylim(top=y_limit)

    if x_axis == "Num Sketches":
        plt.yscale('log')
        plt.ylim(top=5000)
        plt.legend(loc="upper right")
    elif x_axis == "Quantile":
        ax.set_xticklabels(["Other quantiles", "50th quantile"])
        plt.legend(loc="upper left")

    plt.xticks(rotation=0)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.title(plot_title)
    plt.savefig(plot_filename)
    plt.clf()
    print("Finished generating plots.")


def produce_imq_bar_plot_wt_error_bars(data_df, plot_title, x_axis, x_label, y_label, plot_filename, y_limit=0.0):
    mean_data_df = data_df
    if x_axis == "Quantile":
        mean_data_df = data_df.drop('Run ID', axis=1).groupby("Quantile", as_index=False).mean()
    if plot_title == "Insertion time for each data point":
        mean_data_df = data_df.groupby("Data Size", as_index=False).mean()
    elif plot_title == "Merge time per sketch":
        mean_data_df = data_df.groupby("Num Sketches", as_index=False).mean()

    print(mean_data_df)

    if plot_title == "Insertion time for each data point":
        x_ci = data_df.groupby('Data Size', as_index=False).agg(
            lambda x: np.sqrt(x.pow(2).mean() - pow(x.mean(), 2)) * 1.96 / np.sqrt(x.size))
    elif plot_title == "Merge time per sketch":
        x_ci = data_df.groupby('Num Sketches', as_index=False).agg(
            lambda x: np.sqrt(x.pow(2).mean() - pow(x.mean(), 2)) * 1.96 / np.sqrt(x.size))
    else:
        x_ci = data_df.drop('Run ID', axis=1).groupby('Quantile', as_index=False).agg(
            lambda x: np.sqrt(x.pow(2).mean() - pow(x.mean(), 2)) * 1.96 / np.sqrt(x.size))
    print(x_ci)

    ax = mean_data_df.plot(x=x_axis, y=["Moments", "DDS", "UDDS", "KLL", "REQ"], kind="bar")
    for i, alg in enumerate(["Moments", "DDS", "UDDS", "KLL", "REQ"]):
        offset = -0.2 + i * 0.1
        ax.errorbar(mean_data_df.index + offset, mean_data_df[alg], yerr=x_ci[alg], ecolor='k', capsize=3,
                    linestyle="None")
    if y_limit > 0:
        plt.yscale('log')
        plt.ylim(top=y_limit)

    if x_axis == "Num Sketches":
        plt.yscale('log')
        plt.ylim(top=5000)
        plt.legend(loc="upper right")
    elif x_axis == "Quantile":
        ax.set_xticklabels(["Other quantiles", "50th quantile"])
        plt.legend(loc="upper left")

    plt.xticks(rotation=0)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.title(plot_title)
    plt.savefig(plot_filename)
    plt.clf()
    print("Finished generating plots.")


def produce_imq_line_plot(data_df, plot_title, x_axis, x_label, y_label, plot_filename):
    prop_cycle = plt.rcParams['axes.prop_cycle']
    colors = prop_cycle.by_key()['color']

    ax = data_df.plot(x=x_axis, y=["Moments"], style=['o-'],
                      color=[colors[0]])
    if x_axis == "Kurtosis":
        ax.set_xscale('symlog')
        # ax.set_yscale('symlog', linthresh=1e-3)
        plt.xlim(-2, 1000000)
        # plt.ylim(-1e-4, 0.2)
    elif x_axis == "Data Size":
        ax.set_xscale('log')
        ax.set_yscale('log')
        plt.ylim(bottom=1)

    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.title(plot_title)
    plt.savefig(plot_filename.replace(".png", "_moments_only.png"))
    plt.clf()

    ax = data_df.plot(x=x_axis, y=["DDS", "UDDS", "KLL", "REQ"], style=['v-', '^-', '|--', 'x--'],
                      color=[colors[1], colors[2], colors[3], colors[4]])
    if x_axis == "Kurtosis":
        ax.set_xscale('symlog')
        # ax.set_yscale('symlog', linthresh=1e-3)
        plt.xlim(-2, 1000000)
        # plt.ylim(-1e-4, 0.2)
    elif x_axis == "Data Size":
        ax.set_xscale('log')
        ax.set_yscale('log')
        plt.ylim(bottom=1)

    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.title(plot_title)
    plt.savefig(plot_filename.replace(".png", "_wo_moments.png"))
    plt.clf()
    print("Finished generating plots.")


def produce_imq_single_kurt_line_plot(data_df, plot_title, x_axis, x_label, y_label, plot_filename):
    ax = data_df.plot(x=x_axis, y=["Moments", "DDS", "UDDS", "KLL", "REQ"], style=['o-', 'v-', '^-', '|--', 'x--'])
    ax.set_xscale('symlog', linthresh=10)
    plt.xlim(-4, 500000)
    # plt.ylim(0, 0.04)

    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.title(plot_title)
    plt.savefig(plot_filename)
    plt.clf()
    print("Finished generating plots.")


def produce_imq_line_err_plot(data_df, plot_title, x_axis, x_label, y_label, plot_filename):
    prop_cycle = plt.rcParams['axes.prop_cycle']
    colors = prop_cycle.by_key()['color']
    mean_data_df = data_df.groupby('Data Size', as_index=False).mean().round(4)
    x_ci = data_df.groupby('Data Size', as_index=False).agg(
        lambda x: np.sqrt(x.pow(2).mean() - pow(x.mean(), 2)) * 1.96 / np.sqrt(x.size))
    fmt_style = {'Moments': 'o-', 'DDS': 'v-', 'UDDS': '^-', 'KLL': '|--', 'REQ': 'x--'}
    print(x_ci)
    fig, ax = plt.subplots()
    for i, alg in enumerate(["Moments", "DDS", "UDDS", "KLL", "REQ"]):
        # if alg == "Moments" or alg == "REQ":
        #     continue
        ax.plot(mean_data_df[x_axis], mean_data_df[alg], fmt_style[alg], label=alg, color=colors[i])
        plt.errorbar(mean_data_df[x_axis], mean_data_df[alg], yerr=x_ci[alg],
                     ecolor='k', capsize=3, linestyle="None")

    ax.legend(loc="center left")

    if x_axis == "Data Size":
        ax.set_xscale('log')

    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.title(plot_title)
    plt.savefig(plot_filename)
    plt.clf()
    print("Finished generating plots.")


def produce_imq_scatter_plot(data_df, plot_title, x_axis, x_label, y_label, plot_filename):
    size = 20
    fig, ax = plt.subplots()
    for i, alg in enumerate(["DDS", "UDDS", "KLL", "REQ", "Moments"]):
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


def produce_imq_scatter_err_plot(data_df, plot_title, x_axis, x_label, y_label, plot_filename):
    mean_data_df = data_df.groupby('Data Size', as_index=False).mean().round(4)
    x_ci = data_df.groupby('Data Size', as_index=False).agg(
        lambda x: np.sqrt(x.pow(2).mean() - pow(x.mean(), 2)) * 1.96 / np.sqrt(x.size))
    fmt_style = {'Moments': 'o-', 'DDS': 'v-', 'UDDS': '^-', 'KLL': '|--', 'REQ': 'x--'}
    print(x_ci)
    fig, ax = plt.subplots()
    for i, alg in enumerate(["Moments", "DDS", "UDDS", "KLL", "REQ"]):
        ax.errorbar(mean_data_df[x_axis], mean_data_df[alg], label=alg, yerr=x_ci[alg], fmt=fmt_style[alg],
                    ecolor='k', capsize=3)

    ax.legend(loc="upper left")

    if x_axis == "Kurtosis":
        plt.xscale('symlog')
        plt.xlim(-2, 1000000)
        plt.ylim(0, 0.08)
    elif x_axis == "Data Size":
        plt.xscale('log')
        # plt.yscale('log')

    # plt.xticks(rotation=0)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.title(plot_title)
    # plt.legend()
    plt.savefig(plot_filename)
    plt.clf()
    print("Finished generating plots.")


if __name__ == '__main__':
    report_folder = sys.argv[1]
    pd.set_option('display.max_columns', 12)

    # graphs_to_plot = ['query', 'insert', 'merge', 'query_time_ci', 'kurtosis', 'adaptability']
    graphs_to_plot = ['insert', 'merge']
    plot_file_ext = '.pdf'
    display_ci = False

    if 'query' in graphs_to_plot:
        f_query_times = report_folder + '/query_times.csv'
        query_times_df = pd.read_csv(f_query_times)
        query_times_df = query_times_df.groupby('Data Size', as_index=False).mean().round(4)
        print(query_times_df)
        q_plot_file_name = report_folder + '/plots/query_times' + plot_file_ext
        produce_imq_line_plot(query_times_df, 'Query time', "Data Size", 'Data size', 'Time (microseconds)',
                              q_plot_file_name)

    if 'insert' in graphs_to_plot:
        f_insert_times = report_folder + '/insert_times.csv'
        insert_times_df = pd.read_csv(f_insert_times)
        insert_times_df.iloc[:, 1:] = insert_times_df.iloc[:, 1:].div(insert_times_df['Data Size'], axis=0)
        insert_times_df['Data Size'] = insert_times_df['Data Size'] // 1000000
        print(insert_times_df)
        if display_ci:
            i_plot_file_name = report_folder + '/plots/insert_times_ci' + plot_file_ext
            produce_imq_bar_plot_wt_error_bars(insert_times_df, 'Insertion time for each data point', "Data Size",
                                               'Data size (millions)',
                                               'Time (microseconds)', i_plot_file_name)
        else:
            i_plot_file_name = report_folder + '/plots/insert_times' + plot_file_ext
            produce_imq_bar_plot(insert_times_df, 'Insertion time for each data point', "Data Size",
                                 'Data size (millions)',
                                 'Time (microseconds)', i_plot_file_name)

    if 'merge' in graphs_to_plot:
        f_merge_times = report_folder + '/merge_times.csv'
        merge_times_df = pd.read_csv(f_merge_times)
        merge_times_df['Num Merges'] = merge_times_df['Num Sketches'] - 1
        merge_times_df.iloc[:, 1:6] = merge_times_df.iloc[:, 1:6].div(merge_times_df['Num Merges'], axis=0).round(4)
        merge_times_df.drop('Num Merges', axis=1, inplace=True)
        print(merge_times_df)
        if display_ci:
            m_plot_file_name = report_folder + '/plots/merge_times_ci' + plot_file_ext
            produce_imq_bar_plot_wt_error_bars(merge_times_df, 'Merge time per sketch', "Num Sketches",
                                               'Number of sketches', 'Time (microseconds)', m_plot_file_name)
        else:
            m_plot_file_name = report_folder + '/plots/merge_times' + plot_file_ext
            produce_imq_bar_plot(merge_times_df, 'Merge time per sketch', "Num Sketches", 'Number of sketches',
                                 'Time (microseconds)', m_plot_file_name)

    if 'kurtosis' in graphs_to_plot:
        f_kurtosis = report_folder + '/kurtosis.csv'
        kurtosis_df = pd.read_csv(f_kurtosis)
        print(kurtosis_df)
        kurtosis_df.iloc[:, 3:] = kurtosis_df.iloc[:, 3:].sub(kurtosis_df['Actual'], axis=0).div(
            kurtosis_df['Actual'], axis=0).abs()
        print(kurtosis_df.round(6))
        kurtosis_df = kurtosis_df.groupby(['Data Set', 'Kurtosis', 'Actual'], as_index=False).mean().round(6)
        kurtosis_df = kurtosis_df.sort_values(by=['Kurtosis'])
        print(kurtosis_df)
        k_plot_file_name = report_folder + '/plots/kurtosis_accuracy' + plot_file_ext
        produce_imq_single_kurt_line_plot(kurtosis_df, 'Kurtosis vs Accuracy', "Kurtosis", 'Kurtosis',
                                          'Avg. Relative Error (98th percentile)', k_plot_file_name)

    if 'adaptability' in graphs_to_plot:
        f_adaptability = report_folder + '/adaptability.csv'
        adaptability_df = pd.read_csv(f_adaptability)
        adaptability_df.drop(
            adaptability_df[(adaptability_df['Quantile'] == 0.01) | (adaptability_df['Quantile'] == 0.99)].index,
            inplace=True)
        adaptability_df.iloc[:, 2:] = adaptability_df.iloc[:, 2:].sub(adaptability_df['Actual'], axis=0).div(
            adaptability_df['Actual'], axis=0).abs().round(4)
        adaptability_df = adaptability_df.drop('Actual', axis=1)
        adaptability_df = adaptability_df.groupby(['Run ID', 'Quantile'], as_index=False).mean().round(4)
        print(adaptability_df)
        adaptability_df.loc[adaptability_df['Quantile'] != 0.5, 'Quantile'] = 0
        adaptability_df.loc[adaptability_df['Quantile'] == 0.5, 'Quantile'] = 1
        adaptability_df = adaptability_df.groupby(['Run ID', 'Quantile'], as_index=False).mean().round(4)
        print(adaptability_df)
        if display_ci:
            a_plot_file_name = report_folder + '/plots/adaptability_avg_errors_ci' + plot_file_ext
            produce_imq_bar_plot_wt_error_bars(adaptability_df, 'Adaptability', "Quantile", 'Quantiles',
                                               'Avg. Relative Error', a_plot_file_name)
        else:
            a_plot_file_name = report_folder + '/plots/adaptability_avg_errors' + plot_file_ext
            produce_imq_bar_plot(adaptability_df, 'Adaptability', "Quantile", 'Quantiles',
                                 'Avg. Relative Error', a_plot_file_name)

    if 'query_time_ci' in graphs_to_plot:
        f_query_time_tests = report_folder + '/query_times.csv'
        query_time_tests_df = pd.read_csv(f_query_time_tests)
        print(query_time_tests_df)
        q_plot_file_name = report_folder + '/plots/query_time_ci' + plot_file_ext
        produce_imq_line_err_plot(query_time_tests_df, 'Query time', "Data Size", 'Data size', 'Time (microseconds)',
                                  q_plot_file_name)
