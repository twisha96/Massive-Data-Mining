from collections import defaultdict
import argparse
import itertools


# Only get the items that crosses support threshold
# In other words, prune the non-frequent items
def get_frequent_items(item_counts, support_threshold):
	frequent_items = {}
	for item, count in item_counts.iteritems():
		if count > support_threshold:
			frequent_items[item] = count
	return frequent_items


# Get a set of k-tuples which are frequent
def get_k_frequent_itemsets(baskets, k, frequent_item_sets, support_threshold):
	if k==2:
		items_set = set([f for f in frequent_item_sets])
	else:
		items_set = set([f for fp in frequent_item_sets for f in fp])

	item_counts = defaultdict(int)
	for basket in baskets:
		filtered_basket = sorted([item for item in basket if item in items_set])
		k_itemset = itertools.combinations(filtered_basket, k)

		for item in k_itemset:
			item_counts[item] += 1

	return get_frequent_items(item_counts, support_threshold)


# Compute confidence given the prior and posterior probabilities
def confidence(x, union):
    return float(union)/x


# Compute confidence of all rules possible
# k=2 for rules like X -> Y
# k=3 for rules like (X, Y) -> Z
def compute_confidence(frequent_item_sets, k):
	t = []

	if k == 2:
	    for pair, support in frequent_item_sets[k-1].items():
	        t.append(((pair[0], pair[1]), confidence(frequent_item_sets[k-2][pair[0]], support)))  # conf(X=>Y)
	        t.append(((pair[1], pair[0]), confidence(frequent_item_sets[k-2][pair[1]], support)))  # conf(Y=>X)
	else:
	    for k_tuple, support in frequent_item_sets[k-1].items():
	        ikeys = itertools.combinations(k_tuple, k-1)
	        for ikey in ikeys:
				# pdb.set_trace()
				if ikey in frequent_item_sets[k-2]:
					t.append((tuple(list(ikey) + list(set(k_tuple) - set(ikey))), \
						confidence(frequent_item_sets[k-2][ikey], support)))
	return t


def main(args):

	# Read input file in form of baskets and compute single element counts
	baskets = []
	all_items = defaultdict(int)
	frequent_item_sets = []
	support_threshold = args.s
	print "Default threshold: " + str(args.s) + " (Use '-s 120' to change threshold to 120)"

	with open(args.input_file, 'r') as browsing_history:
		for browsing_entry in browsing_history:
			basket = browsing_entry.strip().split(" ")
			for item in basket:
				all_items[item] += 1
			baskets.append(set(basket))

	# Compute frequent itemset using the given support value
	frequent_singles = get_frequent_items(all_items, support_threshold)
	frequent_item_sets.append(frequent_singles)

	# Compute frequent pairs
	k = 2
	frequent_pairs = get_k_frequent_itemsets(baskets, k, frequent_item_sets[k-2], support_threshold)
	frequent_item_sets.append(frequent_pairs)

	# Compute triples
	k = 3
	frequent_triples = get_k_frequent_itemsets(baskets, k, frequent_item_sets[k-2], support_threshold)
	frequent_item_sets.append(frequent_triples)

	# Rules X => Y
	rule_x_y = compute_confidence(frequent_item_sets, k=2)
	rule_x_y.sort(key=lambda x: (-x[1], x[0]))

	print "Top 5 pairs by confidence"
	for pair in rule_x_y[:5]:
		print "{} -> {} {}".format(pair[0][0], pair[0][1], pair[1])

	# Rules (X, Y) => Z
	rule_xy_z = compute_confidence(frequent_item_sets, k=3)
	rule_xy_z.sort(key=lambda x: (-x[1], x[0]))

	print "Top 5 pairs by confidence"
	for triple in rule_xy_z[:5]:
		print "{}, {} -> {} {}".format(triple[0][0], triple[0][1], triple[0][2], triple[1])


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_file', type=str,
    					required=True,
                        # default='homework1/data/browsing.txt',
                        help='input file path [%(default)s]')
    parser.add_argument('--s', type=int,
                        default=100,
                        help='support threshold value [%(default)d]')
    args = parser.parse_args()
    main(args)

'''
Results:

Top 5 pairs by confidence
DAI93865 -> FRO40251 1.0
GRO85051 -> FRO40251 0.999176276771
GRO38636 -> FRO40251 0.990654205607
ELE12951 -> FRO40251 0.990566037736
DAI88079 -> FRO40251 0.986725663717

Top 5 pairs by confidence
DAI23334, ELE92920 -> DAI62779 1.0
DAI31081, GRO85051 -> FRO40251 1.0
DAI55911, GRO85051 -> FRO40251 1.0
DAI62779, DAI88079 -> FRO40251 1.0
DAI75645, GRO85051 -> FRO40251 1.0
'''
