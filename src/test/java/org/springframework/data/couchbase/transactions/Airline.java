package org.springframework.data.couchbase.transactions;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.couchbase.repository.TransactionMeta;

class Airline {
	@Id public String id;
	public String name;
	public long numAircraft;
	@Version // repository.save() needs version to distinguish insert/replace/upsert for tx/no-tx
	public long version;
	@TransactionMeta Integer txResultKey; // needed for immutable and copied objects

	public Airline(String id, String name) {
		this.id = id;
		this.name = name;
	}

	Airline copy() { // copies the txResultKey as well
		Airline b = new Airline(id, name);
		b.version = version;
		b.txResultKey = txResultKey;
		return b;
	}

	Airline withName(String newName) { // with-er to change the name
		Airline b = copy();
		b.name = newName;
		return b;
	}
	Airline withNumAircraft(long numAircraft) { // with-er to change the name
		Airline b = copy();
		b.numAircraft = numAircraft;
		return b;
	}

	public String toString() {
    return "{ id: " + id + " name: \"" + name + "\" version: " + version + ", txResultkey: " + txResultKey + " }";
  }

}
