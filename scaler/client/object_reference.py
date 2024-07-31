import dataclasses


@dataclasses.dataclass
class ObjectReference(object):
    name: bytes
    object_id: bytes
    size: int

    def __repr__(self):
        return f"ScalerReference(name={self.name}, id={self.object_id}, size={self.size})"

    def __hash__(self):
        return hash(self.object_id)

    def __eq__(self, other: "ObjectReference"):
        if not isinstance(other, ObjectReference):
            return False

        return self.object_id == other.object_id

    def __ne__(self, other):
        return not self.__eq__(other)
