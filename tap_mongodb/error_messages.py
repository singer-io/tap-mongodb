"""Helpers for building tenant-safe log and error messages.

Anything the MongoDB server sends back is untrusted input: a tenant may point a
source at a host they fully control. Echoing that content into the tap's stderr
is dangerous for two reasons.

1. Server supplied text is copied verbatim into ``pymongo`` exception messages,
   so a bug in the driver's BSON decoder can place bytes that never came from
   the server (for example adjacent process memory) into an error that Stitch
   shows to the source owner.
2. Stitch treats any stderr line starting with ``CRITICAL ``/``FATAL `` as the
   user visible job error, so a server supplied string containing a newline can
   forge one of those lines and surface arbitrary content.

Every value that originates from the server must therefore be passed through
``scrub`` before it is logged, and ``safe_error_message`` must be used instead
of ``str(exception)`` for anything that ends up in a ``CRITICAL`` line.
"""

import logging
import re

MAX_MESSAGE_LENGTH = 300

REDACTION = '<redacted>'

# ``b'...'`` / ``b"..."`` reprs are how decoded BSON Binary values (the payload
# that can carry out-of-bounds heap bytes) show up inside exception messages.
_BYTES_LITERAL = re.compile(r"b(['\"]).*?\1", re.DOTALL)

# ``\xd9``-style escapes encode raw bytes using otherwise allowed characters, so
# they have to be removed before the character allowlist is applied.
_ESCAPE_SEQUENCE = re.compile(
    r"\\+(?:x[0-9a-fA-F]{2}|u[0-9a-fA-F]{4}|U[0-9a-fA-F]{8}|N\{[^}]*\}|[0-7]{1,3}|.)"
)

# Deliberately excludes the backslash and every whitespace character other than
# a plain space, so scrubbed text cannot re-introduce escapes or extra log lines.
_DISALLOWED_CHARACTERS = re.compile(r"[^A-Za-z0-9 .,:;_()\[\]{}/@=+*<>'\"?!#$%&|~^`-]")

# Static, server independent explanations for the driver errors tenants actually
# hit. Keyed on exception class name so that the tenant still gets an actionable
# message without any server supplied text being reflected back.
_PYMONGO_ERROR_DESCRIPTIONS = {
    'AutoReconnect':
        'Lost the connection to the MongoDB server.',
    'ConfigurationError':
        'The MongoDB connection is misconfigured. Check the replica set name, '
        'authentication database and SSL settings.',
    'ConnectionFailure':
        'Could not connect to the MongoDB server. Check the host, port and '
        'network/firewall configuration.',
    'CursorNotFound':
        'The MongoDB server closed the cursor before the extraction finished.',
    'ExecutionTimeout':
        'A MongoDB operation exceeded its time limit.',
    'InvalidBSON':
        'The MongoDB server returned a malformed BSON response.',
    'NetworkTimeout':
        'Timed out communicating with the MongoDB server.',
    'NotPrimaryError':
        'The MongoDB server is not the primary member of the replica set.',
    'OperationFailure':
        'The MongoDB server rejected an operation.',
    'ProtocolError':
        'The MongoDB server returned a malformed response.',
    'ServerSelectionTimeoutError':
        'Could not reach a usable MongoDB server. Check the host, port, '
        'replica set name and network/firewall configuration.',
}

# ``OperationFailure.code`` is a server supplied integer, so it is safe to
# report and lets support identify the failure without the server's ``errmsg``.
_OPERATION_FAILURE_CODES = {
    13: 'the user is not authorized to perform this operation',
    18: 'authentication failed',
    303: 'the feature is not supported by this MongoDB server version',
}

_GENERIC_PYMONGO_DESCRIPTION = 'The MongoDB connection failed.'

_OMITTED_SUFFIX = ('The full server response has been omitted because it is '
                   'untrusted input.')


def scrub(value, max_length=MAX_MESSAGE_LENGTH):
    """Return a single line, printable-ASCII rendering of ``value``.

    Bytes reprs and backslash escape sequences are replaced wholesale rather
    than character by character, so raw byte values cannot survive as their hex
    representation.
    """
    text = _BYTES_LITERAL.sub(REDACTION, str(value))
    text = _ESCAPE_SEQUENCE.sub(REDACTION, text)
    text = _DISALLOWED_CHARACTERS.sub(' ', text)
    text = ' '.join(text.split())

    if len(text) > max_length:
        text = text[:max_length].rstrip() + '...'

    return text


def safe_error_message(exception):
    """Return a message for ``exception`` that is safe to show to the tenant.

    ``pymongo`` exceptions embed the server's error document, so only the
    exception class and the numeric error code are reported for those. Messages
    raised by the tap itself are scrubbed instead, because they may still
    interpolate server supplied values such as collection names.
    """
    name = type(exception).__name__

    if _is_pymongo_error(exception):
        description = _PYMONGO_ERROR_DESCRIPTIONS.get(name, _GENERIC_PYMONGO_DESCRIPTION)

        details = [name]
        code = getattr(exception, 'code', None)
        if isinstance(code, int) and not isinstance(code, bool):
            details.append('code {}'.format(code))
            reason = _OPERATION_FAILURE_CODES.get(code)
            if reason:
                details.append(reason)

        return '{} ({}). {}'.format(description, ', '.join(details), _OMITTED_SUFFIX)

    message = scrub(exception)
    if not message:
        return name

    return '{}: {}'.format(name, message)


def _is_pymongo_error(exception):
    # Imported lazily and defensively so that a failure to import the driver is
    # still reported through this module rather than blowing up the handler.
    try:
        from pymongo.errors import PyMongoError  # pylint: disable=import-outside-toplevel
        from bson.errors import BSONError  # pylint: disable=import-outside-toplevel
    except ImportError:  # pragma: no cover
        return False

    return isinstance(exception, (PyMongoError, BSONError))


# Stitch reads the tap's stderr one line at a time and decides what a line is
# from its leading token, so a value containing a line break can forge a whole
# log record: a `CRITICAL ` line becomes the job's user visible error and an
# `INFO METRIC: ` line is parsed as a metric.
_FORGED_LINE = re.compile(
    r"^(?:CRITICAL|FATAL|ERROR|WARNING|WARN|INFO|DEBUG|NOTSET|METRIC)\b"
)


def neutralize_line_forgery(message):
    """Indent any continuation line that could be read as a new log record.

    Multi-line log messages are used deliberately by the tap, so the lines are
    kept; they are only made unable to impersonate the start of a record. Exotic
    line separators are normalised to ``\\n`` because Stitch splits lines with
    ``str.splitlines``, which treats them as breaks too.
    """
    lines = message.splitlines()
    if len(lines) < 2:
        return message

    return '\n'.join(
        [lines[0]] + [' ' + line if _FORGED_LINE.match(line) else line for line in lines[1:]]
    )


class LineForgeryFilter(logging.Filter):  # pylint: disable=too-few-public-methods
    """Applies :func:`neutralize_line_forgery` to every record that is emitted."""

    def filter(self, record):
        message = record.getMessage()
        neutralized = neutralize_line_forgery(message)

        if neutralized != message:
            record.msg = neutralized
            record.args = ()

        return True


def install_line_forgery_filter():
    """Attach :class:`LineForgeryFilter` to the handlers singer configured.

    The filter lives on the handlers rather than on a logger so that records
    from every module in the tap, and from singer itself, pass through it.
    """
    log_filter = LineForgeryFilter()

    for handler in logging.getLogger().handlers:
        handler.addFilter(log_filter)

    return log_filter
