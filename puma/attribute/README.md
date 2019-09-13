## Auto-managed Attribute Scopes

PUMA provides a means to declare the scope of attributes within classes that span multiple execution environments.
Once the scope is declared like this, the attribute can be accessed just like a regular attribute of an object (obviously only from within the allowed execution environment(s)).
This allows user code to define cross-environment sharing properties of attributes cleanly without having to write boilerplate code.
It is done by defining the attribute at the class level with one of the scope identifiers, as follows:

```python
class MyClass:
    _scoped_attr_1: int = child_only('_scoped_attr_1')
```

The attribute can then be defined (as is ordinarily done) in the constructor (`__init__`).
Obviously, when doing this, user code should respect the scope defined earlier, and in applicable cases (documented below) use the appropriate delegation methods.
We outline the available attribute scopes in the sections below with examples.

### `manually_managed`

TODO

### `unmanaged`

TODO

### `copied`

TODO

### `child_only`

TODO

### `parent_only`
