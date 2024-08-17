import dataclasses


@dataclasses.dataclass
class ObjectReference:
    name: bytes
    object_id: bytes
    size: int

    def __repr__(self):
        return f"ScalerReference(name={self.name!r}, id={self.object_id!r}, size={self.size})"

    def __hash__(self):
        return hash(self.object_id)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ObjectReference):
            return NotImplemented

        return self.object_id == other.object_id

    def __ne__(self, other):
        return not self.__eq__(other)
