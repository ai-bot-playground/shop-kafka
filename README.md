# shop-kafka

Repozytorium infrastruktury Kafki dla całego systemu. Odpowiada za trzy rzeczy:
konfigurację brokera (tryb **KRaft**, bez Zookeepera), **definicje tematów** oraz
**bibliotekę kontraktów zdarzeń** współdzieloną przez serwisy.

## Co tu trzymać

- Konfiguracja brokera (zmienne `KAFKA_*` — patrz docker-compose w shop-infra).
- Definicje tematów (liczba partycji, retencja) jako kod / skrypt inicjujący.
- **Biblioteka kontraktów**: schematy zdarzeń (DTO / Avro / JSON Schema) jako
  wersjonowany artefakt, który `shop-order`, `shop-inventory`, `shop-payment` i
  `shop-notification` dodają jako zależność. Dzięki temu wszystkie serwisy mówią
  tym samym „językiem" zdarzeń, a zmiana schematu jest kontrolowana w jednym miejscu.

## Tematy i partycje

| Temat               | Partycje | Klucz       | Po co tyle partycji |
|---------------------|----------|-------------|---------------------|
| order-events        | 6        | orderId     | równoległe zamówienia |
| inventory-events    | 6        | productId   | kolejność zdarzeń **per produkt**, różne produkty równolegle |
| payment-events      | 6        | orderId     | równoległe płatności |
| `<temat>.DLT`       | 1        | —           | Dead Letter Topic |

Klucz partycji jest kluczowy: `productId` na `inventory-events` powoduje, że
wszystkie zdarzenia jednego produktu trafiają na tę samą partycję → są
przetwarzane w kolejności, co chroni przed wyścigami przy rezerwacji.

`shop-notification` **nie ma osobnego tematu** — konsumuje terminalne zdarzenia
`OrderConfirmed` / `OrderCancelled` / `OrderRejected` wprost z `order-events`
(własna grupa konsumenta). Gdyby trzeba było odseparować ruch powiadomień, można
w przyszłości dołożyć dedykowany temat wraz z producentem komend powiadomień.

## Ustawienia producenta (do zaimplementowania w serwisach)

- `acks=all`, `enable.idempotence=true` — brak duplikatów i utraty przy retry.
- Outbox pattern: serwis zapisuje zdarzenie do tabeli `outbox` w tej samej
  transakcji co zmianę stanu; osobny publisher (poller lub Debezium/CDC) wysyła
  je do Kafki. Gwarantuje spójność „zapis w bazie ⇔ publikacja zdarzenia".
- Serializacja: JSON (Jackson) lub Avro/Protobuf + Schema Registry przy rozbudowie.

## Ustawienia konsumenta (do zaimplementowania)

- Osobny `group.id` per serwis: `shop-inventory`, `shop-order`, `shop-payment`,
  `shop-notification`.
- Dostarczanie *at-least-once* → konsument **musi być idempotentny**
  (tabela `processed_events` z deduplikacją po `eventId`).
- Obsługa błędów: retry z backoffem, po wyczerpaniu prób → publikacja na `*.DLT`
  (Spring Kafka: `DefaultErrorHandler` + `DeadLetterPublishingRecoverer`).

## Produkcja

Replication factor ≥ 3, `min.insync.replicas=2`, `acks=all` — inaczej tracisz
zdarzenia przy awarii brokera. W demie wszędzie 1.

## Kafka UI (narzędzie deweloperskie)

W docker-compose działa `kafka-ui` (http://localhost:8081) do podglądu tematów,
treści zdarzeń i **consumer lag** — rosnący lag to sygnał, że trzeba dołożyć
konsumentów lub partycje. Przydaje się też do obserwacji przepływu sagi i
zawartości tematów `*.DLT`.
