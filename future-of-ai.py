from __future__ import annotations
from abc import ABC, abstractmethod
from typing import TypeVar, Generic, Protocol, ClassVar, Final, runtime_checkable
from dataclasses import dataclass, field
from functools import reduce, wraps
from enum import Enum, auto
import sys

T = TypeVar("T")
S = TypeVar("S", bound="Stringifiable")
M = TypeVar("M", bound="MessageCarrier")

# ──────────────────────────────────────────────
# PROTOCOLS
# ──────────────────────────────────────────────

@runtime_checkable
class Stringifiable(Protocol):
    def to_str(self) -> str: ...

@runtime_checkable
class MessageCarrier(Protocol):
    def carry(self) -> "Payload": ...

# ──────────────────────────────────────────────
# ENUMS
# ──────────────────────────────────────────────

class Salutation(Enum):
    HELLO = auto()
    GREETINGS = auto()
    SALUTATIONS = auto()

class Target(Enum):
    WORLD = auto()
    UNIVERSE = auto()
    EXISTENCE = auto()

# ──────────────────────────────────────────────
# GENERICS & ABSTRACT BASE
# ──────────────────────────────────────────────

@dataclass
class Payload(Generic[T]):
    value: T
    metadata: dict[str, object] = field(default_factory=dict)

    def transform(self, fn: "type[Payload[S]]") -> "Payload[S]":
        return fn(self.value)  # type: ignore[arg-type]

class AbstractMessageFactory(ABC, Generic[T]):
    _registry: ClassVar[dict[str, type]] = {}

    def __init_subclass__(cls, tag: str = "", **kwargs: object) -> None:
        super().__init_subclass__(**kwargs)
        if tag:
            AbstractMessageFactory._registry[tag] = cls

    @abstractmethod
    def build(self) -> Payload[T]: ...

    @classmethod
    def from_registry(cls, tag: str) -> "AbstractMessageFactory[str]":
        return cls._registry[tag]()

# ──────────────────────────────────────────────
# DECORATORS
# ──────────────────────────────────────────────

def singleton(cls: type[T]) -> type[T]:
    instances: dict[type, object] = {}
    @wraps(cls)  # type: ignore[arg-type]
    def get_instance(*args: object, **kwargs: object) -> T:
        if cls not in instances:
            instances[cls] = cls(*args, **kwargs)
        return instances[cls]  # type: ignore[return-value]
    return get_instance  # type: ignore[return-value]

def validated(method):
    @wraps(method)
    def wrapper(self, *args, **kwargs):
        result = method(self, *args, **kwargs)
        assert isinstance(result, Payload), "Must return a Payload"
        return result
    return wrapper

# ──────────────────────────────────────────────
# WORD ATOMS
# ──────────────────────────────────────────────

@dataclass(frozen=True)
class WordAtom:
    phonemes: tuple[str, ...]

    @classmethod
    def from_string(cls, s: str) -> "WordAtom":
        return cls(phonemes=tuple(s))

    def reconstruct(self) -> str:
        return "".join(self.phonemes)

@dataclass(frozen=True)
class SalutationAtom(WordAtom):
    salutation: Salutation = Salutation.HELLO

@dataclass(frozen=True)
class TargetAtom(WordAtom):
    target: Target = Target.WORLD

# ──────────────────────────────────────────────
# FACTORIES
# ──────────────────────────────────────────────

class HelloFactory(AbstractMessageFactory[str], tag="hello"):
    CANONICAL: Final[str] = "Hello"

    @validated
    def build(self) -> Payload[str]:
        atom = SalutationAtom.from_string(self.CANONICAL)
        return Payload(value=atom.reconstruct(), metadata={"salutation": Salutation.HELLO})

class WorldFactory(AbstractMessageFactory[str], tag="world"):
    CANONICAL: Final[str] = "World"

    @validated
    def build(self) -> Payload[str]:
        atom = TargetAtom.from_string(self.CANONICAL)
        return Payload(value=atom.reconstruct(), metadata={"target": Target.WORLD})

# ──────────────────────────────────────────────
# COMBINATOR
# ──────────────────────────────────────────────

class PayloadCombinator(Generic[T]):
    def __init__(self, payloads: list[Payload[T]], separator: str = " ") -> None:
        self._payloads = payloads
        self._separator = separator

    def combine(self) -> Payload[T]:
        combined: T = self._separator.join(  # type: ignore[assignment]
            p.value for p in self._payloads  # type: ignore[union-attr]
        )
        merged_meta = reduce(lambda a, b: {**a, **b}, (p.metadata for p in self._payloads), {})
        return Payload(value=combined, metadata=merged_meta)

# ──────────────────────────────────────────────
# OUTPUT PIPELINE
# ──────────────────────────────────────────────

@runtime_checkable
class Emittable(Protocol):
    def emit(self) -> None: ...

@singleton
class ConsoleEmissionSingleton:
    def __init__(self) -> None:
        self._stream = sys.stdout

    def emit(self, payload: Payload[str]) -> None:
        self._stream.write(payload.value + "\n")
        self._stream.flush()

class EmissionPipeline(Generic[M]):
    def __init__(self, emitter: ConsoleEmissionSingleton) -> None:
        self._emitter = emitter
        self._middleware: list[type] = []

    def add_middleware(self, mw: type) -> "EmissionPipeline[M]":
        self._middleware.append(mw)
        return self

    def run(self, payload: Payload[str]) -> None:
        self._emitter.emit(payload)

# ──────────────────────────────────────────────
# ORCHESTRATOR
# ──────────────────────────────────────────────

class HelloWorldOrchestrator:
    def __init__(self) -> None:
        self._registry = AbstractMessageFactory._registry
        self._emitter = ConsoleEmissionSingleton()
        self._pipeline: EmissionPipeline[MessageCarrier] = EmissionPipeline(self._emitter)

    def execute(self) -> None:
        tags: list[str] = ["hello", "world"]
        factories = [AbstractMessageFactory.from_registry(tag) for tag in tags]
        payloads: list[Payload[str]] = [f.build() for f in factories]
        combined = PayloadCombinator(payloads).combine()
        self._pipeline.run(combined)

# ──────────────────────────────────────────────
# ENTRY POINT
# ──────────────────────────────────────────────

if __name__ == "__main__":
    HelloWorldOrchestrator().execute()
