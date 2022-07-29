/*
 * Copyright 2022 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.example.demo;

import com.couchbase.client.java.transactions.TransactionResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.stereotype.Component;

/**
 * Components of the type CommandLineRunner are called right after the application start up. So the method *run* is
 * called as soon as the application starts.
 * 
 * @author Michael Reiche
 */
@Component
public class CmdRunner implements CommandLineRunner {

	@Autowired AirportRepository airportRepository;
	@Autowired CouchbaseTemplate template;
	@Autowired CouchbaseClientFactory couchbaseClientFactory;
	@Autowired AirportService airportService;

	@Override
	public void run(String... strings) throws Exception {

		try {
			template.removeById(Airport.class).one("1");
		} catch (Exception e){}
		try {
			template.removeById(Airport.class).one("2");
		} catch (Exception e){}

		Airport new_1 = new Airport("1", "JFK", "jfk");
		Airport saved_1 = airportRepository.save(new_1);
		Airport found_1 = airportRepository.findById(saved_1.getId()).get();
		System.out.println("found using repository by id: " + found_1);

		Airport new_2 = new Airport("2", "LGA", "lga");
		Airport saved_2 = airportRepository.save(new_2);
		Airport found_2 = airportRepository.findById(saved_2.getId()).get();
		System.out.println("found using repository by id: " + found_2);
		Airport found_3 = airportRepository.findByIata("JFK");
		System.out.println("founding using repository findByIata: "+found_3);


		airportService.transferGates(new_1.getId(), new_2.getId(), 50);

		System.out.println("found after transferGates: "+airportRepository.findById(saved_1.getId()).get());
		System.out.println("found after transferGates: "+airportRepository.findById(saved_2.getId()).get());
	}


	public void transferGatesDeprecated(String fromId, String toId, int gatesToTransfer) {
		TransactionResult txResult = template.getCouchbaseClientFactory().getCluster().transactions().run(ctx -> {

			Airport fromAirport = template.findById(Airport.class).one(fromId);
			Airport toAirport = template.findById(Airport.class).one(toId);
			toAirport.gates += gatesToTransfer;
			fromAirport.gates -= gatesToTransfer;
			template.save(fromAirport);
			template.save(toAirport);
		});

	}

}
