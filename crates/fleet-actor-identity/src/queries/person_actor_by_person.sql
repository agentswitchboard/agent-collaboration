SELECT person.actor_id, actor.actor_kind
FROM actor_persons AS person
LEFT JOIN actor_identities AS actor ON actor.actor_id = person.actor_id
WHERE person.person_id = ?
