class Either:
    def __init__(self, *args, **kwargs):

        # at least 2 arguments. Assume first two are Left and Right
        if len(args) >= 2:
            self.left = args[0]
            self.right = args[1]
            # 1 argument. May be 'dict like'
        elif len(args) == 1:
            if isinstance(args[0], Either):
                self.left = args[0].left
                self.right = args[0].right
            else:
                try:
                    self.right = args[0].get("right")
                    self.left = args[0].get("left")
                except:
                    try:
                        self.right = args[0].right
                        self.left = args[0].left
                    except:
                        pass

                if self.left is None and self.right is None:
                    # only 1 value, Just assume it's a 'right'
                    self.right = args[0]

        # No positional arguments - could be provided via kwargs
        else:
            self.left = kwargs.get("left", None)
            self.right = kwargs.get("right", None)

    # Call the left function if left is available
    # Otherwise call the right function
    def match(self, left, right):
        if self.left is not None:
            return left(self.left)

        return right(self.right)

    def is_left(self):
        return self.left is not None

    def if_left(self, left):
        if self.left is not None:
            return left(self.left)

    def is_right(self):
        return self.right is not None

    def if_right(self, right):
        if self.right is not None:
            return right(self.right)

    def __getattr__(self, name):
        return self.__dict__.get(name)

    def bind(self, fn):
        return Either(fn(self.right)) if self.left is None else self

    def Left(v):
        return Either(left=v)

    def Right(v):
        return Either(right=v)
