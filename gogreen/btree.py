# -*- Mode: Python; tab-width: 4 -*-

# Copyright (c) 2010 Slide, Inc.
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

            # first, try to pass a value left
            if parent_index:
                left = parent.children[parent_index - 1]
                if len(left.values) < left.order:
                    parent.neighbor_pass_left(self, parent_index)
                    return

            # second, try to pass one right
            if parent_index + 1 < len(parent.children):
                right = parent.children[parent_index + 1]
                if len(right.values) < right.order:
                    parent.neighbor_pass_right(self, parent_index)
                    return

        # finally, split the current node, then shrink the parent if we must
        center = len(self.values) // 2
        median = self.values[center]

        # create a sibling node with the second half of our data
        args = [self.tree, self.values[center + 1:]]
        if self.BRANCH:
            args.append(self.children[center + 1:])
        sibling = type(self)(*args)

        # cut our data down to the first half
        self.values = self.values[:center]
        if self.BRANCH:
            self.children = self.children[:center + 1]

        if not parent:
            parent = self.tree.BRANCH_NODE(self.tree, [], [self])
            parent_index = 0
            self.tree._root = parent

        # pass the median element up to the parent
        parent.values.insert(parent_index, median)
        parent.children.insert(parent_index + 1, sibling)
        if len(parent.values) > parent.order:
            parent.shrink(path)

    def grow(self, path, count=1):
        parent, parent_index = path.pop()
        minimum = self.order // 2
        left, right = None, None

        # first try to borrow from the right sibling
        if parent_index + 1 < len(parent.children):
            right = parent.children[parent_index + 1]
            if len(right.values) - count >= minimum:
                parent.neighbor_pass_left(right, parent_index + 1, count)
                return

        # then try borrowing from the left sibling
        if parent_index:
            left = parent.children[parent_index - 1]
            if len(left.values) - count >= minimum:
                parent.neighbor_pass_right(left, parent_index - 1, count)
                return

        # see if we can borrow a few from both
        if count > 1 and left and right:
            lspares = len(left.values) - minimum
            rspares = len(right.values) - minimum
            if lspares + rspares >= count:
                # distribute the pulling evenly between the two neighbors
                even_remaining = lspares + rspares - count
                from_right = rspares - (even_remaining // 2)
                parent.neighbor_pass_right(left, parent_index - 1, from_left)
                parent.neighbor_pass_left(right, parent_index + 1, from_right)
                return

        # consolidate with a sibling -- try left first
        if left:
            left.values.append(parent.values.pop(parent_index - 1))
            left.values.extend(self.values)
            if self.BRANCH:
                left.children.extend(self.children)
            parent.children.pop(parent_index)
        else:
            self.values.append(parent.values.pop(parent_index))
            self.values.extend(right.values)
            if self.BRANCH:
                self.children.extend(right.children)
            parent.children.pop(parent_index + 1)

        if len(parent.values) < minimum:
            if path:
                # parent is not the root
                parent.grow(path)
            elif not parent.values:
                # parent is root and is now empty
                self.tree._root = left or self

    def __repr__(self):
        name = self.BRANCH and "BRANCH" or "LEAF"
        return "<%s %s>" % (name, ", ".join(map(str, self.values)))


class BTreeBranchNode(BTreeNode):
    BRANCH = True
    __slots__ = ["tree", "order", "values", "children"]

    def __init__(self, tree, values, children):
        self.tree = tree
        self.order = tree.order
        self.values = values
        self.children = children

    def neighbor_pass_right(self, child, child_index, count=1):
        separator_index = child_index
        target = self.children[child_index + 1]
        index = len(child.values) - count

        target.values[0:0] = (child.values[index + 1:] +
                [self.values[separator_index]])
        self.values[separator_index] = child.values[index]
        child.values[index:] = []

        if child.BRANCH:
            target.children[0:0] = child.children[-count:]
            child.children[-count:] = []

    def neighbor_pass_left(self, child, child_index, count=1):
        separator_index = child_index - 1
        target = self.children[child_index - 1]

        target.values.extend([self.values[separator_index]] +
                child.values[:count - 1])
        self.values[separator_index] = child.values[count - 1]
        child.values[:count] = []

        if child.BRANCH:
            index = len(child.values) + 1
            target.children.extend(child.children[:count])
            child.children[:count] = []

    def remove(self, index, path):
        minimum = self.order // 2

        # try replacing the to-be removed item from the right subtree first
        to_leaf = [(self, index + 1)]
        descendent = self.children[index + 1]
        while descendent.BRANCH:
            to_leaf.append((descendent, 0))
            descendent = descendent.children[0]
        if len(descendent.values) > minimum:
            path.extend(to_leaf)
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
        self.values[index] = descendent.values[-1]
        descendent.remove(len(descendent.values) - 1, path)

    def split(self, value):
        index = bisect.bisect_right(self.values, value)
        child = self.children[index]

        left = type(self)(self.tree, self.values[:index], self.children[:index])

        self.values = self.values[index:]
        self.children = self.children[index + 1:]

        # right here both left and self has the same number of children as
        # values -- but the relevant child hasn't been split yet, so we'll add
        # the two resultant children to the respective child list

        left_child, right_child = child.split(value)
        left.children.append(left_child)
        self.children.insert(0, right_child)

        return left, self


class BTreeLeafNode(BTreeNode):
    BRANCH = False
    __slots__ = ["tree", "order", "values"]

    def __init__(self, tree, values):
        self.tree = tree
        self.order = tree.order
        self.values = values

    def remove(self, index, path):
        self.values.pop(index)
        if path and len(self.values) < self.order // 2:
            self.grow(path)

    def split(self, value):
        index = bisect.bisect_right(self.values, value)

        left = type(self)(self.tree, self.values[:index])

        self.values = self.values[index:]

        self.tree._first = self

        return left, self


class BTree(object):
    BRANCH_NODE = BTreeBranchNode
    LEAF_NODE = BTreeLeafNode

    def __init__(self, order):
        self.order = order
        self._root = self._first = self.LEAF_NODE(self, [])

    def __nonzero__(self):
        return bool(self._root.values)

    @property
    def first(self):
        if not self:
            return None
        return self._first.values[0]

    def insert(self, value, after=False):
        path = self.find_path_to_leaf(value, after)
        node, index = path.pop()

        node.values.insert(index, value)

        if len(node.values) > self.order:
            node.shrink(path)

    def remove(self, value, last=True):
        test = last and self._test_right or self._test_left
        path = self.find_path(value, last)
        node, index = path.pop()

        if test(node.values, index, value):
            if last:
                index -= 1
            node.remove(index, path)
        else:
            raise ValueError("%r not in %s" % (value, self.__class__.__name__))

    def __repr__(self):
        def recurse(node, accum, depth):
            accum.append(("  " * depth) + repr(node))
            if node.BRANCH:
                for child in node.children:
                    recurse(child, accum, depth + 1)

        accum = []
        recurse(self._root, accum, 0)
        return "\n".join(accum)

    def _test_right(self, values, index, value):
        return index and values[index - 1] == value

    def _test_left(self, values, index, value):
        return index < len(values) and values[index] == value

    def find_path(self, value, after=False):
        cut = after and bisect.bisect_right or bisect.bisect_left
        test = after and self._test_right or self._test_left

        path, node = [], self._root
        index = cut(node.values, value)
        path.append((node, index))

        while node.BRANCH and not test(node.values, index, value):
            node = node.children[index]
            index = cut(node.values, value)
            path.append((node, index))

        return path

    def find_path_to_leaf(self, value, after=False):
        cut = after and bisect.bisect_right or bisect.bisect_left

        path = self.find_path(value, after)
        node, index = path[-1]

        while node.BRANCH:
            node = node.children[index]
            index = cut(node.values, value)
            path.append((node, index))

        return path

    def _iter_recurse(self, node):
        if node.BRANCH:
            for child, value in itertools.izip(node.children, node.values):
                for ancestor_value in self._iter_recurse(child):
                    yield ancestor_value

                yield value

            for value in self._iter_recurse(node.children[-1]):
                yield value
        else:
            for value in node.values:
                yield value

    def __iter__(self):
        return self._iter_recurse(self._root)

    def pull_prefix(self, value):
        '''
        get and remove the prefix section of the btree up to and
        including all values for `value`, and return it as a list

        http://www.chiark.greenend.org.uk/~sgtatham/tweak/btree.html#S6.2
        '''
        left, right = self._root.split(value)

        # first eliminate redundant roots
        while self._root.BRANCH and not self._root.values:
            self._root = self._root.children[0]

        # next traverse down, rebalancing as we go
        if self._root.BRANCH:
            path = [(self._root, 0)]
            node = self._root.children[0]

            while node.BRANCH:
                short_by = (node.order // 2) - len(node.values)
                if short_by > 0:
                    node.grow(path[:], short_by + 1)
                path.append((node, 0))
                node = node.children[0]

            short_by = (node.order // 2) - len(node.values)
            if short_by > 0:
                node.grow(path[:], short_by + 1)

        throwaway = object.__new__(type(self))
        throwaway._root = left # just using you for your __iter__
        return iter(throwaway)

    @classmethod
    def bulkload(cls, values, order):
        tree = object.__new__(cls)
        tree.order = order

        minimum = order // 2
        valuegroups, separators = [[]], []

        for value in values:
            if len(valuegroups[-1]) < order:
                valuegroups[-1].append(value)
            else:
                separators.append(value)
                valuegroups.append([])

        if len(valuegroups[-1]) < minimum and separators:
            sep_value = separators.pop()
            last_two_values = valuegroups[-2] + [sep_value] + valuegroups[-1]
            valuegroups[-2] = last_two_values[:minimum]
            valuegroups[-1] = last_two_values[minimum + 1:]
            separators.append(last_two_values[minimum])

        last_generation = []
        for values in valuegroups:
            last_generation.append(cls.LEAF_NODE(tree, values))

        tree._first = last_generation[0]

        if not separators:
            tree._root = last_generation[0]
            return tree

        while len(separators) > order + 1:
            pairs, separators = separators, []
            last_values, values = values, [[]]

            for value in pairs:
                if len(values[-1]) < order:
                    values[-1].append(value)
                else:
                    separators.append(value)
                    values.append([])

            if len(values[-1]) < minimum and separators:
                sep_value = separators.pop()
                last_two_values = values[-2] + [sep_value] + values[-1]
                values[-2] = last_two_values[:minimum]
                values[-1] = last_two_values[minimum + 1:]
                separators.append(last_two_values[minimum])

            offset = 0
            for i, value_group in enumerate(values):
                children = last_generation[offset:offset + len(value_group) + 1]
                values[i] = cls.BRANCH_NODE(tree, value_group, children)
                offset += len(value_group) + 1

            last_generation = values

        root = cls.BRANCH_NODE(tree, separators, last_generation)

        tree._root = root
        return tree
