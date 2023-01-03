import MQTTJs from "mqtt";
import {fromEvent, merge, map, firstValueFrom, Subject, pipe, filter, throwError, share, first, skipUntil, tap, BehaviorSubject, connect, ReplaySubject} from "rxjs";
import { randomUUID } from "crypto";

const matchTopicFilter = (filter, topic) => {
	// AWS deviates from the spec as # can't match the parent topic
	// ex: sensor/# does not receive messages for sensor
	// https://docs.aws.amazon.com/iot/latest/developerguide/topics.html#topicfilters
	//
	// This function matches the AWS version
	const filterParts = filter.split("/");
	const topicParts = topic.split("/");
	return filterParts.length <= topicParts.length &&
		topicParts.every((p, i) =>
			filterParts[i] === p ||
			["+", "#"].includes(filterParts[i]) ||
			(filterParts.length <= i && filterParts[filterParts.length - 1] === "#")
		);

}

const connectMqtt = async (opts) => {
	const client  = MQTTJs.connect(opts);
	await firstValueFrom(merge(
		fromEvent(client, "error").pipe(map((e) => {throw e})),
		fromEvent(client, "connect"),
	));

	const client$ = new Subject();

	const {IOT_ENDPOINT, CA, CERT, KEY, THING_NAME} = process.env;

	const SUBSCRIBE = Symbol("SUBSCRIBE");

	const subscriptions$ = client$.pipe(filter(({operation}) => operation === SUBSCRIBE), map(({identifier, args: {topic, options}}) => {
		const connected$ = new ReplaySubject();
		const messages$ = fromEvent(client, "message").pipe(
			skipUntil(connected$),
			share({
				connector: () => new Subject(),
				resetOnRefCountZero: false,
			}),
			//tap((a) => console.log(a)),
			filter((ev) => matchTopicFilter(topic, ev[0])),
			map(([, message]) => message.toString()),
		);
		client.subscribe(topic, options, (err, granted) => {
			if (err) {
				connected$.next(throwError(() => err));
			}else {
				console.log("connected fires")
				connected$.next(granted);
			}
		});
		return {identifier, messages$, connected$};
	}));

	const subscribe = (topic, options) => async (fn) => {
		const identifier = randomUUID();
		const [{messages$, connected$}] = await Promise.all([
			firstValueFrom(subscriptions$.pipe(filter((e) => e.identifier === identifier))),
			client$.next({operation: SUBSCRIBE, identifier, args: {topic, options}}),
		]);
		await firstValueFrom(connected$)
		await fn(messages$);
	}

	await subscribe(`$aws/things/${THING_NAME}/shadow/name/test/update/accepted`)((a) => {
		console.log(a)
		a.subscribe((b) => console.log(b))
		console.log("publish")
		client.publish(`$aws/things/${THING_NAME}/shadow/name/test/update`, JSON.stringify({state: {reported: {test: "abcd"}}}));
	});

	/*
	subscribe(`$aws/things/${THING_NAME}/shadow/name/+/update/accepted`, undefined, (err, granted) => {
		console.log(err)
		client.publish(`$aws/things/${THING_NAME}/shadow/name/test/update`, JSON.stringify({state: {reported: {test: "abc"}}}));
		client.addListener("message", (topic, message, payload) => {
			console.log("MESSAGE")
			console.log(topic, message, payload)
		})
	});

	await setTimeout(5000);
	*/
	return {
		subscribe,
	}
};
