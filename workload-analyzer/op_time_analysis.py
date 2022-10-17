import sys

import matplotlib.pyplot as plt
import matplotlib as mpl
import numpy as np
import pandas as pd
import matplotlib.style as style
from matplotlib.ticker import ScalarFormatter

style.use('tableau-colorblind10')


def produce_imq_bar_plot(data_df, plot_title, x_axis, x_label, y_label, plot_filename, y_limit=0.0):
    if x_axis == "Quantile":
        data_df = data_df.drop('Run ID', axis=1).groupby("Quantile", as_index=False).mean()
    if plot_title == "Insertion time for each data point":
        data_df = data_df.groupby("Data Size", as_index=False).mean()
    elif plot_title == "Merge time per sketch":
        data_df = data_df.groupby("Num Sketches", as_index=False).mean()

    print(data_df)

    ax = data_df.plot(x=x_axis, y=["KLL", "Moments", "DDS", "UDDS", "REQ"], kind="bar")
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

    if plot_title == "Adaptability":
        fig, ax = plt.subplots(figsize=(4, 3))
    else:
        fig, ax = plt.subplots(figsize=(5, 3))
    mean_data_df.plot(x=x_axis, y=["KLL", "Moments", "DDS", "UDDS", "REQ"], kind="bar", ax=ax)
    for i, alg in enumerate(["KLL", "Moments", "DDS", "UDDS", "REQ"]):
        offset = -0.2 + i * 0.1
        ax.errorbar(mean_data_df.index + offset, mean_data_df[alg], yerr=x_ci[alg], ecolor='k', capsize=3,
                    linestyle="None")

    bars = ax.patches
    if plot_title == "Insertion time for each data point":
        hatches = ["....", "....", "....", "\\\\\\\\", "\\\\\\\\", "\\\\\\\\", "////", "////", "////", "", "",
                   "", "xxxx", "xxxx", "xxxx"]
    else:
        hatches = ["....", "....", "\\\\\\\\", "\\\\\\\\", "////", "////", "", "", "xxxx", "xxxx"]

    for i, (bar, hatch) in enumerate(zip(bars, hatches)):
        bar.set_hatch(hatch)

    if y_limit > 0:
        plt.yscale('log')
        plt.ylim(top=y_limit)

    if x_axis == "Num Sketches":
        plt.yscale('log')
        plt.ylim(top=5000)
        ax.legend()
    elif x_axis == "Quantile":
        ax.set_xticklabels(["Other quantiles", "50th quantile"])
        ax.legend(loc="upper left")

    if plot_title == "Insertion time for each data point":
        ax.legend(loc="upper left", bbox_to_anchor=(0.23, 0.55))
    plt.xticks(rotation=0)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    # plt.title(plot_title)
    fig.tight_layout()
    ax.autoscale(enable=True)
    plt.savefig(plot_filename)
    plt.clf()
    print("Finished generating plots.")


def produce_long_legend(data_df, plot_filename):
    fig, ax = plt.subplots(figsize=(12, 0.5))
    mean_data_df = data_df.groupby("Data Size", as_index=False).mean()

    print(mean_data_df)

    mean_data_df.plot(y=["KLL", "Moments", "DDS", "UDDS", "REQ"], kind="bar", ax=ax)

    bars = ax.patches
    hatches = ["....", "....", "....", "....", "\\\\\\\\", "\\\\\\\\", "\\\\\\\\", "\\\\\\\\",
               "////", "////", "////", "////", "", "", "", "", "xxxx", "xxxx", "xxxx", "xxxx"]

    for i, (bar, hatch) in enumerate(zip(bars, hatches)):
        bar.set_hatch(hatch)

    data_df.plot(y=["KLL", "Moments", "DDS", "UDDS", "REQ"], style=['o-', 'v-', '^-', '|--', 'x--'], ax=ax)
    plt.ylim(top=5000)

    ax.plot(np.NaN, np.NaN, '-', color='w', label=' ')
    ax.legend()
    handles, labels = ax.get_legend_handles_labels()
    order = [6, 7, 8, 9, 10, 5, 0, 1, 2, 3, 4]
    ax.legend([handles[idx] for idx in order], [labels[idx] for idx in order],
              ncol=11, fancybox=True, loc="center")

    for bar in bars:
        bar.set_visible(False)

    lines = ax.get_lines()
    for line in lines:
        line.set_visible(False)
    ax.grid(False)

    # Hide axes ticks
    ax.set_xticks([])
    ax.set_yticks([])
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['bottom'].set_visible(False)
    ax.spines['left'].set_visible(False)
    fig.tight_layout()
    ax.autoscale(enable=True)
    plt.savefig(plot_filename)
    plt.show()
    plt.clf()
    print("Finished generating plots.")


def produce_short_legend(data_df, plot_filename):
    fig, ax = plt.subplots(figsize=(9, 0.5))
    mean_data_df = data_df.groupby("Data Size", as_index=False).mean()

    print(mean_data_df)

    mean_data_df.plot(y=["KLL", "Moments", "DDS", "UDDS", "REQ"], kind="bar", ax=ax)

    bars = ax.patches
    hatches = ["....", "....", "....", "....", "\\\\\\\\", "\\\\\\\\", "\\\\\\\\", "\\\\\\\\",
               "////", "////", "////", "////", "", "", "", "", "xxxx", "xxxx", "xxxx", "xxxx"]

    for i, (bar, hatch) in enumerate(zip(bars, hatches)):
        bar.set_hatch(hatch)

    data_df.plot(y=["KLL", "Moments", "DDS", "UDDS", "REQ"], style=['o-', 'v-', '^-', '|--', 'x--'], ax=ax)
    plt.ylim(top=5000)

    ax.plot(np.NaN, np.NaN, '-', color='w', label=' ')
    ax.plot(np.NaN, np.NaN, '-', color='w', label=' ')
    ax.plot(np.NaN, np.NaN, '-', color='w', label=' ')
    ax.plot(np.NaN, np.NaN, '-', color='w', label=' ')
    # ax.legend(ncol=14)
    handles, labels = ax.get_legend_handles_labels()
    mod_labels = []
    for i, label in enumerate(labels):
        if i > 8:
            mod_labels.append("")
        else:
            mod_labels.append(label)
    order = [9, 0, 5, 10, 1, 6, 11, 2, 7, 12, 3, 8, 13, 4]
    print(mod_labels)
    ax.legend([handles[idx] for idx in order], [mod_labels[idx] for idx in order],
              ncol=14, fancybox=True, loc="center", handletextpad=0.5, columnspacing=0.5,
              frameon=False)

    for bar in bars:
        bar.set_visible(False)

    lines = ax.get_lines()
    for line in lines:
        line.set_visible(False)
    ax.grid(False)

    # Hide axes ticks
    ax.set_xticks([])
    ax.set_yticks([])
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['bottom'].set_visible(False)
    ax.spines['left'].set_visible(False)
    fig.tight_layout()
    ax.autoscale(enable=True)
    plt.savefig(plot_filename)
    plt.show()
    plt.clf()
    print("Finished generating plots.")


def produce_imq_line_plot(data_df, plot_title, x_axis, x_label, y_label, plot_filename):
    fig, ax = plt.subplots(figsize=(5, 3))
    data_df.plot(x=x_axis, y=["KLL", "Moments", "DDS", "UDDS", "REQ"], style=['o-', 'v-', '^-', '|--', 'x--'], ax=ax)
    if x_axis == "Kurtosis":
        ax.set_xscale('symlog')
        plt.xlim(-2, 1000000)
    elif x_axis == "Data Size":
        ax.set_xscale('log')
        ax.xaxis.set_major_formatter(ScalarFormatter())
        ax.ticklabel_format(style='plain')

    plt.xlabel(x_label)
    plt.ylabel(y_label)
    # plt.title(plot_title)
    fig.tight_layout()
    # ax.autoscale(enable=True)
    plt.savefig(plot_filename.replace(".png", "_wo_moments.png"))
    plt.clf()
    print("Finished generating plots.")


def produce_imq_single_kurt_line_plot(data_df, plot_title, x_axis, x_label, y_label, plot_filename):
    ax = data_df.plot(x=x_axis, y=["KLL", "Moments", "DDS", "UDDS", "REQ"], style=['o-', 'v-', '^-', '|--', 'x--'])
    ax.set_xscale('symlog', linthresh=10)
    plt.xlim(-4, 500000)
    # plt.ylim(0, 0.04)

    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.title(plot_title)
    plt.savefig(plot_filename)
    plt.clf()
    print("Finished generating plots.")


def produce_imq_bar_plot_kurtosis(data_df, plot_title, x_axis, x_label, y_label, plot_filename, y_limit=0.0):
    prop_cycle = plt.rcParams['axes.prop_cycle']

    mean_data_df = data_df.groupby(['Data Set', 'Kurtosis', 'Actual'], as_index=False).mean().round(6)
    mean_data_df = mean_data_df.sort_values(by=['Kurtosis']).reset_index(drop=True).round({'Kurtosis': 2})
    mean_data_df['xtick_label'] = mean_data_df['Kurtosis'].astype("string") + '\n(' + mean_data_df['Data Set'] + ')'
    print(mean_data_df)

    x_ci = data_df.groupby(['Data Set', 'Kurtosis', 'Actual'], as_index=False).agg(
        lambda x: 0.0 if (x.unique().size == 1) else (
                np.sqrt(x.pow(2).mean() - pow(x.mean(), 2)) * 1.96 / np.sqrt(x.size)))
    x_ci = x_ci.sort_values(by=['Kurtosis']).reset_index(drop=True)
    print(x_ci)

    fig, ax = plt.subplots(figsize=(6, 3))
    mean_data_df.plot(x=x_axis, y=["KLL", "Moments", "DDS", "UDDS", "REQ"], ax=ax, kind="bar")
    ax.set_xticklabels(mean_data_df['xtick_label'])
    for i, alg in enumerate(["KLL", "Moments", "DDS", "UDDS", "REQ"]):
        offset = -0.2 + i * 0.1
        print(x_ci[alg])
        ax.errorbar(mean_data_df.index + offset, mean_data_df[alg], yerr=x_ci[alg], ecolor='k', capsize=3,
                    linestyle="None")

    bars = ax.patches
    hatches = ["....", "....", "....", "....", "\\\\\\\\", "\\\\\\\\", "\\\\\\\\", "\\\\\\\\", "////", "////", "////",
               "////", "", "", "", "", "xxxx", "xxxx", "xxxx", "xxxx"]

    for i, (bar, hatch) in enumerate(zip(bars, hatches)):
        bar.set_hatch(hatch)

    plt.xticks(rotation=0)
    ax.set_xlabel(x_label)
    ax.set_ylabel(y_label)
    ax.legend()
    fig.tight_layout()
    ax.autoscale(enable=True)
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
    for i, alg in enumerate(["KLL", "Moments", "DDS", "UDDS", "REQ"]):
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


if __name__ == '__main__':
    report_folder = sys.argv[1]
    pd.set_option('display.max_columns', 12)
    mpl.rcParams['hatch.linewidth'] = 0.5

    # graphs_to_plot = ['query', 'insert', 'merge', 'query_time_ci', 'kurtosis', 'adaptability']
    graphs_to_plot = ['query', 'insert', 'merge', 'legend']
    plot_file_ext = '.pdf'
    display_ci = True
    short_legend = True

    if 'legend' in graphs_to_plot:
        f_query_times = report_folder + '/query_times.csv'
        query_times_df = pd.read_csv(f_query_times)
        query_times_df = query_times_df.groupby('Data Size', as_index=False).mean().round(4)
        query_times_df['Data Size'] = query_times_df['Data Size'] // 1000000
        print(query_times_df)
        if short_legend:
            legend_plot_file_name = report_folder + '/plots/legend_a' + plot_file_ext
            produce_short_legend(query_times_df, legend_plot_file_name)
        else:
            legend_plot_file_name = report_folder + '/plots/legend_b' + plot_file_ext
            produce_long_legend(query_times_df, legend_plot_file_name)

    if 'query' in graphs_to_plot:
        f_query_times = report_folder + '/query_times.csv'
        query_times_df = pd.read_csv(f_query_times)
        query_times_df = query_times_df.groupby('Data Size', as_index=False).mean().round(4)
        query_times_df['Data Size'] = query_times_df['Data Size'] // 1000000
        print(query_times_df)
        q_plot_file_name = report_folder + '/plots/query_times' + plot_file_ext
        produce_imq_line_plot(query_times_df, 'Query time', "Data Size", 'Number of entries (millions)',
                              'Time (microseconds)',
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
                                               'Number of entries (millions)',
                                               'Time (microseconds)', i_plot_file_name)
        else:
            i_plot_file_name = report_folder + '/plots/insert_times' + plot_file_ext
            produce_imq_bar_plot(insert_times_df, 'Insertion time for each data point', "Data Size",
                                 '#Entries (millions)',
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
        k_plot_file_name = report_folder + '/plots/kurtosis_accuracy' + plot_file_ext
        produce_imq_bar_plot_kurtosis(kurtosis_df, 'Kurtosis vs Accuracy', "Kurtosis", 'Kurtosis',
                                      'Avg. Relative Error', k_plot_file_name)

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
