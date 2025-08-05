// The Swift Programming Language
// https://docs.swift.org/swift-book

import KurrentDB
import Foundation

@main
struct FixDatabase {
    static func main() async throws {
        let settings: ClientSettings = "kurrentdb://localhost:2113?tls=false"
        let client = KurrentDBClient(settings: settings)
        
        let settings2: ClientSettings = "kurrentdb://172.16.100.55:2113?tls=false"
        let client2 = KurrentDBClient(settings: settings2)
        
        var faildResponses: [(Streams<AllStreams>.ReadAll.Response, any Error)] = []
        let responses = try await client.readAllStreams(){
            $0.resolveLinks()
        }
        
        let recordEvents = try await responses.reduce(into: [RecordedEvent]()) { partialResult, response in
            do{
                let record = try response.event.record
                if !record.eventType.hasPrefix("$") && !record.streamIdentifier.name.hasPrefix("$") {
                    partialResult.append(record)
                }else {
                    print("skipped record: \(record.eventType), \(record.streamIdentifier)")
                }
            }catch {
                faildResponses.append((response, error))
            }
        }

        let sortedEvents = recordEvents.sorted { (lhs, rhs) in
            guard let lhsJSON = try? JSONSerialization.jsonObject(with: lhs.data) as? [String: Any],
                  let rhsJSON = try? JSONSerialization.jsonObject(with: rhs.data) as? [String: Any],
                  let lhsTimeInterval = lhsJSON["occurred"] as? Double,
                  let rhsTimeInterval = rhsJSON["occurred"] as? Double
            else {
                return false
            }
            return lhsTimeInterval < rhsTimeInterval
        }
        
        for recordedEvent in sortedEvents {
            print(recordedEvent.streamIdentifier)
            let eventData = EventData(id: recordedEvent.id, like: recordedEvent)
            try await client2.appendStream(recordedEvent.streamIdentifier, events: [eventData])
        }

    }
}
