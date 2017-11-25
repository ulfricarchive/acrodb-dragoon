package com.ulfric.dragoon.acrodb;

import java.util.Objects;

import com.ulfric.acrodb.Bucket;
import com.ulfric.dragoon.ObjectFactory;
import com.ulfric.dragoon.application.Container;
import com.ulfric.dragoon.extension.inject.Inject;
import com.ulfric.dragoon.qualifier.Qualifier;
import com.ulfric.dragoon.stereotype.Stereotypes;

public class AcrodbContainer extends Container {

	@Inject
	private ObjectFactory factory;

	public AcrodbContainer() {
		addBootHook(this::bindAcrodb);
		addShutdownHook(this::unbindAcrodb);
	}

	private void bindAcrodb() {
		Bucket acrodb = new Bucket();
		factory.bind(Bucket.class).toFunction(parameters -> {
			Qualifier qualifier = parameters.getQualifier();
			if (qualifier != null) {
				Database database = Stereotypes.getFirst(qualifier, Database.class);

				if (database != null) {
					Bucket bucket = acrodb;
					for (String nextBucket : database.value()) {
						bucket = bucket.openBucket(nextBucket);
					}
					return bucket;
				}
			}

			return acrodb;
		});

		factory.bind(Store.class).toFunction(parameters -> {
			Objects.requireNonNull(parameters, "parameters");

			Qualifier qualifier = parameters.getQualifier();
			Objects.requireNonNull(qualifier, "qualifier");

			@SuppressWarnings("unchecked")
			Class<? extends Document> type = (Class<? extends Document>) qualifier.getType();

			Bucket bucket = factory.request(Bucket.class, parameters);

			return new Store<>(bucket, type);
		});
	}

	private void unbindAcrodb() {
		factory.bind(Bucket.class).toNothing();
		factory.bind(Store.class).toNothing();
	}

}