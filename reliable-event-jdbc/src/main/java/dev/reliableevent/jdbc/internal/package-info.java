/**
 * Internal JDBC implementation details, not part of the user-facing API.
 *
 * <p>Dependencies flow from model and retry policy into persistence, then into
 * publication and recovery, with cycle orchestration at the outermost layer.</p>
 */
package dev.reliableevent.jdbc.internal;
