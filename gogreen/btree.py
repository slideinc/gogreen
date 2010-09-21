# -*- Mode: Python; tab-width: 4 -*-

# Copyright (c) 1999, 2000 by eGroups, Inc.
# Copyright (c) 2005-2010 Slide, Inc.
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are
# met:
#
#     * Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above
#       copyright notice, this list of conditions and the following
#       disclaimer in the documentation and/or other materials provided
#       with the distribution.
#     * Neither the name of the author nor the names of other
#       contributors may be used to endorse or promote products derived
#       from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
# A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
# OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
# THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import bisect
import itertools
import operator


class BTreeNode(object):
    def shrink(self, path):
        parent = None
        if path:
            parent, parent_index = path.pop()

            # first, try to pass a (key, value) left
            if parent_index:
                left = parent.children[parent_index - 1]
                if len(left.keys) < left.order:
                    parent.neighbor_pass_left(self, parent_index)
                    return

            # second, try to pass one right
            if parent_index + 1 < len(parent.children):
                right = parent.children[parent_index + 1]
                if len(right.keys) < right.order:
                    parent.neighbor_pass_right(self, parent_index)
                    return

        # finally, split the current node, then shrink the parent if we must
        center = len(self.keys) // 2
        median = self.keys[center], self.values[center]

        # create a sibling node with the second half of our data
        args = [self.tree, self.keys[center + 1:], self.values[center + 1:]]
        if self.BRANCH:
            args.append(self.children[center + 1:])
        sibling = type(self)(*args)

        # cut our data down to the first half
        self.keys = self.keys[:center]
        self.values = self.values[:center]
        if self.BRANCH:
            self.children = self.children[:center + 1]

        if not parent:
            parent = self.tree.BRANCH_NODE(self.tree, [], [], [self])
            parent_index = 0
            self.tree._root = parent

        # pass the median element up to the parent
        parent.keys.insert(parent_index, median[0])
        parent.values.insert(parent_index, median[1])
        parent.children.insert(parent_index + 1, sibling)
        if len(parent.keys) > parent.order:
            parent.shrink()

    def grow(self, path):
        parent, parent_index = path.pop()
        minimum = self.order // 2
        left, right = None, None

        # first try to borrow from the right sibling
        if parent_index + 1 < len(parent.children):
            right = parent.children[parent_index + 1]
            if len(right.keys) > minimum:
                parent.neighbor_pass_left(right, parent_index + 1)
                return

        # fall back to borrowing from the left sibling
        if parent_index:
            left = parent.children[parent_index - 1]
            if len(left.keys) > minimum:
                parent.neighbor_pass_right(left, parent_index - 1)
                return

        # consolidate with a sibling -- try left first
        if left:
            left.keys.append(parent.keys.pop(parent_index - 1))
            left.values.append(parent.values.pop(parent_index - 1))
            left.keys.extend(self.keys)
            left.values.extend(self.values)
            if self.BRANCH:
                left.children.extend(self.children)
            parent.children.pop(parent_index)
        else:
            self.keys.append(parent.keys.pop(parent_index))
            self.values.append(parent.values.pop(parent_index))
            self.keys.extend(right.keys)
            self.values.extend(right.values)
            if self.BRANCH:
                self.children.extend(right.values)
            parent.children.pop(parent_index + 1)

        if len(parent.keys) < minimum:
            if path:
                # parent is not the root
                parent.grow(path)
            elif not parent.keys:
                # parent is root and is now empty
                self.tree._root = left or self

    def __repr__(self):
        name = self.BRANCH and "BRANCH" or "LEAF"
        return "<%s %s>" % (name, ", ".join(map(str, self.keys)))


class BTreeBranchNode(BTreeNode):
    BRANCH = True

    def __init__(self, tree, keys, values, children):
        self.tree = tree
        self.order = tree.order
        self.keys = keys
        self.values = values
        self.children = children

    def neighbor_pass_right(self, child, child_index):
        separator_index = child_index
        target = self.children[child_index + 1]
        target.keys.insert(0, self.keys[separator_index])
        target.values.insert(0, self.values[separator_index])
        self.keys[separator_index] = child.keys.pop()
        self.values[separator_index] = child.values.pop()
        if child.BRANCH:
            target.children.insert(0, child.children.pop())

    def neighbor_pass_left(self, child, child_index):
        separator_index = child_index - 1
        target = self.children[child_index - 1]
        target.keys.append(self.keys[separator_index])
        target.values.append(self.values[separator_index])
        self.keys[separator_index] = child.keys.pop(0)
        self.values[separator_index] = child.values.pop(0)
        if child.BRANCH:
            target.children.append(child.children.pop(0))

    def remove(self, index, path):
        minimum = self.order // 2

        # try replacing the to-be removed item from the right subtree first
        to_leaf = [(self, index + 1)]
        descendent = self.children[index + 1]
        while descendent.BRANCH:
            to_leaf.append((descendent, 0))
            descendent = descendent.children[0]
        if len(descendent.keys) > minimum:
            path.extend(to_leaf)
            self.keys[index] = descendent.keys[0]
            self.values[index] = descendent.values[0]
            descendent.remove(0, path)
            return

        # fall back to promoting from the left subtree
        to_leaf = [(self, index)]
        descendent = self.children[index]
        while descendent.BRANCH:
            to_leaf.append((descendent, len(descendent.children) - 1))
            descendent = descendent.children[-1]
        path.extend(to_leaf)
        self.keys[index] = descendent.keys[-1]
        self.values[index] = descendent.values[-1]
        descendent.remove(len(descendent.children) - 1, path)


class BTreeLeafNode(BTreeNode):
    BRANCH = False

    def __init__(self, tree, keys, values):
        self.tree = tree
        self.order = tree.order
        self.keys = keys
        self.values = values

    def remove(self, index, path):
        self.keys.pop(index)
        self.values.pop(index)
        if path and len(self.keys) < self.order // 2:
            self.grow(path)


class BTree(object):
    BRANCH_NODE = BTreeBranchNode
    LEAF_NODE = BTreeLeafNode

    def __init__(self, order):
        self.order = order
        self._root = self.LEAF_NODE(self, [], [])

    def get(self, key, default=None):
        path = self._find_path(key)
        node, index = path[-1]
        if node.keys[index] == key:
            return node.values[index]
        return default

    __getitem__ = get

    def insert(self, key, value, after=False):
        path = self._find_path_to_leaf(key, after)
        node, index = path.pop()

        node.keys.insert(index, key)
        node.values.insert(index, value)

        if len(node.keys) > self.order:
            node.shrink(path)

    __setitem__ = insert

    def remove(self, key, last=True):
        test = last and self._test_right or self._test_left
        path = self._find_path(key, last)
        node, index = path.pop()
        print index

        if test(node.keys, index, key):
            if last:
                index -= 1
            node.remove(index, path)
        else:
            raise ValueError("%r not in %s" % (item, self.__class__.__name__))

    __delitem__ = remove

    def __repr__(self):
        def recurse(node, accum, depth):
            accum.append(("  " * depth) + repr(node))
            if node.BRANCH:
                for child in node.children:
                    recurse(child, accum, depth + 1)

        accum = []
        recurse(self._root, accum, 0)
        return "\n".join(accum)

    def _test_right(self, keys, index, key):
        return index and keys[index - 1] == key

    def _test_left(self, keys, index, key):
        return index < len(keys) and keys[index] == key

    def _find_path(self, key, after=False):
        cut = after and bisect.bisect_right or bisect.bisect_left
        test = after and self._test_right or self._test_left

        path, node = [], self._root
        index = cut(node.keys, key)
        path.append((node, index))

        while node.BRANCH and not test(node.keys, index, key):
            node = node.children[index]
            index = cut(node.keys, key)
            path.append((node, index))

        return path

    def _find_path_to_leaf(self, key, after=False):
        cut = after and bisect.bisect_right or bisect.bisect_left

        path = self._find_path(key, after)
        node, index = path[-1]

        while node.BRANCH:
            node = node.children[index]
            index = cut(node.keys, key)
            path.append(node, index)

        return path

    def iteritems(self):
        def recurse(node):
            if node.BRANCH:
                for child, key, value in itertools.izip(
                        node.children, node.keys, node.values):
                    for pair in recurse(child):
                        yield pair
                    yield key, value
                for pair in recurse(node.children[-1]):
                    yield pair
            else:
                for pair in itertools.izip(node.keys, node.values):
                    yield pair

        for pair in recurse(self._root):
            yield pair

    def iterkeys(self):
        return itertools.imap(operator.itemgetter(0), self.iteritems())

    def itervalues(self):
        return itertools.imap(operator.itemgetter(0), self.iteritems())

    def items(self):
        return list(self.iteritems())

    def keys(self):
        return [pair[0] for pair in self.iteritems()]

    def values(self):
        return [pair[1] for pair in self.iteritems()]

    __iter__ = iterkeys

    #def pull_prefix(self, key):
    #    '''
    #    get and remove the prefix section of the btree up to and
    #    including all values for `key`, and return it as a list

    #    http://www.chiark.greenend.org.uk/~sgtatham/tweak/btree.html#S6.2
    #    '''
    #    self._root.split(key, None)

    @classmethod
    def bulkload(cls, keys, values, order):
        tree = object.__new__(cls)
        tree.order = order

        minimum = order // 2
        keygroups, valuegroups, separators = [[]], [[]], []

        for key, value in itertools.izip(keys, values):
            if len(keygroups[-1]) < order:
                keygroups[-1].append(key)
                valuegroups[-1].append(value)
            else:
                separators.append((key, value))
                keygroups.append([])
                valuegroups.append([])

        if len(keygroups[-1]) < minimum and separators:
            sep_key, sep_value = separators.pop()
            last_two_keys = keygroups[-2] + [sep_key] + keygroups[-1]
            last_two_values = valuegroups[-2] + [sep_value] + valuegroups[-1]
            keygroups[-2] = last_two_keys[:minimum]
            keygroups[-1] = last_two_keys[minimum + 1:]
            valuegroups[-2] = last_two_values[:minimum]
            valuegroups[-1] = last_two_values[minimum + 1:]
            separators.append(
                    (last_two_keys[minimum], last_two_values[minimum]))

        last_generation = []
        for keys, values in itertools.izip(keygroups, valuegroups):
            last_generation.append(cls.LEAF_NODE(tree, keys, values))

        if not separators:
            tree._root = last_generation[0]
            return tree

        while len(separators) > order + 1:
            pairs, separators = separators, []
            last_keys, keys = keys, [[]]
            last_values, values = values, [[]]

            for key, value in pairs:
                if len(keys[-1]) < order:
                    keys[-1].append(key)
                    values[-1].append(value)
                else:
                    separators.append((key, value))
                    keys.append([])
                    values.append([])

            if len(keys[-1]) < minimum and separators:
                sep_key, sep_value = separators.pop()
                last_two_keys = keys[-2] + [sep_key] + keys[-1]
                last_two_values = values[-2] + [sep_value] + values[-1]
                keys[-2] = last_two_keys[:minimum]
                keys[-1] = last_two_keys[minimum + 1:]
                values[-2] = last_two_values[:minimum]
                values[-1] = last_two_values[minimum + 1:]
                separators.append(
                        (last_two_keys[minimum], last_two_values[minimum]))

            offset = 0
            for i, (key_group, value_group) in enumerate(
                    itertools.izip(keys, values)):
                children = last_generation[offset:offset + len(key_group) + 1]
                keys[i] = cls.BRANCH_NODE(
                        tree, key_group, value_group, children)
                offset += len(key_group) + 1

            last_generation = keys

        root = cls.BRANCH_NODE(
                tree,
                [x[0] for x in separators],
                [x[1] for x in separators],
                last_generation)

        tree._root = root
        return tree