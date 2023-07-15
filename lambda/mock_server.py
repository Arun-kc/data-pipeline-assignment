import random, string
import datetime
import json
import uuid

post_templates = [
    "I just visited {place} and it was {adj}! {positive_phrase}",
    "Having a great time at {event}. It's {adj}! {positive_phrase}",
    "Check out this amazing {noun} I found! {positive_phrase}",
    "Just {verb} an {adj} {noun} and it was {adj}. {positive_phrase}",
    "Feeling {adj} about this {noun} I discovered! {positive_phrase}",
    "Avoiding {place} because it's {adj}. {negative_phrase}",
    "The {event} was {adj}. I wouldn't recommend it. {negative_phrase}",
    "Not impressed with this {noun}. It's {adj}. {negative_phrase}",
    "Had a {adj} {noun} experience. Not worth it. {negative_phrase}",
    "Neutral feelings about {place}. It was {adj}. {neutral_phrase}",
    "Attended the {event} and it was {adj}. Not much to say. {neutral_phrase}",
    "Came across this {noun}. It was {adj}. {neutral_phrase}"
]

comment_templates = [
    "That sounds {adj}. Can't wait to {verb} {noun}! {positive_phrase}",
    "I agree, {place} is always {adj}. {positive_phrase}",
    "Wow, that {noun} looks {adj}! {positive_phrase}",
    "I've been there too! It was {adj}. {positive_phrase}",
    "Thanks for sharing. I'll definitely {verb} that {noun}. {positive_phrase}",
    "Sorry to hear that. {negative_phrase}",
    "I had a different experience at {place}. It was {adj}. {negative_phrase}",
    "Not sure I would enjoy {event}. {negative_phrase}",
    "I don't think I would like that {noun}. {negative_phrase}",
    "Interesting. {neutral_phrase}",
    "I'm neutral about {place}. It's {adj}. {neutral_phrase}",
    "Neutral feelings about the {event}. {neutral_phrase}"
]

positive_phrases = [
    "It made my day!", "I highly recommend it.", "I'm still in awe.", "A truly memorable experience.",
    "I couldn't be happier!", "Definitely worth it.", "A gem I discovered.", "My new favorite.",
    "I'm so glad I went.", "An absolute delight.", "Can't stop thinking about it.", "A positive surprise."
]

negative_phrases = [
    "Disappointing overall.", "Not what I expected.", "I wouldn't go again.", "Left me unsatisfied.",
    "A letdown.", "Wish I had chosen differently.", "Not up to par.", "Underwhelming experience.",
    "Wouldn't recommend it.", "A waste of time.", "Far from impressive.", "Not worth the hype."
]

neutral_phrases = [
    "It was alright.", "Nothing extraordinary.", "A mixed bag.", "Middle of the road.",
    "Didn't leave a lasting impression.", "Decent but not outstanding.", "Neutral vibes.",
    "Can't say I loved it or hated it.", "Nothing remarkable to report.", "Meh, it was average."
]

adjectives = [
    'amazing', 'awesome', 'incredible', 'fantastic', 'wonderful', 'excellent',
    'fabulous', 'phenomenal', 'outstanding', 'impressive', 'marvelous', 'stunning',
    'breathtaking', 'spectacular', 'mind-blowing', 'unforgettable', 'extraordinary',
    'delightful', 'joyful', 'blissful', 'thrilling', 'grateful', 'glorious',
    'radiant', 'euphoric', 'vibrant', 'captivating', 'charming', 'enchanting',
    'refreshing', 'inspiring', 'uplifting', 'empowering', 'heartwarming', 'magical',
    'serene', 'tranquil', 'harmonious', 'rejuvenating', 'resplendent', 'fascinating',
    'remarkable', 'dynamic', 'brilliant', 'innovative', 'adventurous', 'passionate',
    'optimistic', 'graceful', 'energetic', 'engaging', 'sensational', 'enjoyable'
]

places = [
    'New York City', 'Paris', 'Tokyo', 'London', 'Rome', 'Sydney',
    'San Francisco', 'Barcelona', 'Amsterdam', 'Dubai', 'Cape Town', 'Bali',
    'Los Angeles', 'Berlin', 'Rio de Janeiro', 'Hong Kong', 'Florence', 'Prague',
    'Venice', 'Vienna', 'Athens', 'Seoul', 'Toronto', 'Mumbai', 'Hawaii',
    'Montreal', 'Istanbul', 'Moscow', 'San Diego', 'Vancouver', 'Bangkok',
    'Singapore', 'Melbourne', 'Chicago', 'Madrid', 'Lisbon', 'Stockholm',
    'Edinburgh', 'Copenhagen', 'Dublin', 'Auckland', 'Havana', 'Budapest',
    'Shanghai', 'Zurich', 'Dubrovnik', 'Beijing', 'Amman', 'Marrakech', 'Helsinki'
]

events = [
    'music festival', 'art exhibition', 'food fair', 'tech conference',
    'sports event', 'film premiere', 'charity gala', 'fashion show',
    'comedy show', 'wine tasting', 'book signing', 'dance performance',
    'workshop', 'lecture', 'theater play', 'symphony concert', 'opera',
    'science exhibition', 'cultural festival', 'photography exhibition',
    'design conference', 'cooking class', 'business summit', 'car show',
    'flea market', 'yoga retreat', 'wellness workshop', 'hackathon',
    'street fair', 'cosplay event', 'beer festival', 'craft fair',
    'travel expo', 'environmental summit', 'job fair', 'gaming tournament',
    'magic show', 'historical reenactment', 'surfing competition',
    'wine and cheese tasting', 'TEDx event', 'product launch', 'poetry reading',
    'film screening', 'farming workshop', 'political debate', 'fitness bootcamp'
]

nouns = [
    'experience', 'adventure', 'journey', 'discovery', 'vacation', 'trip',
    'getaway', 'expedition', 'exploration', 'excursion', 'retreat', 'outing',
    'escape', 'quest', 'detour', 'safari', 'trek', 'pilgrimage',
    'wanderlust', 'sojourn', 'odyssey', 'passage', 'quest', 'explore',
    'roam', 'sail', 'navigate', 'traverse', 'conquer', 'uncover',
    'learn', 'discover', 'embrace', 'indulge', 'cherish', 'savor',
    'participate', 'immerse', 'engage', 'appreciate', 'admire', 'enjoy',
    'capture', 'experience', 'encounter', 'apprehend', 'absorb', 'grasp'
]

verbs = [
    'visited', 'explored', 'discovered', 'experienced', 'enjoyed', 'embraced',
    'indulged in', 'participated in', 'took part in', 'ventured into', 'tried out',
    'engaged with', 'immersed in', 'dissected', 'analyzed', 'appreciated', 'uncovered',
    'understood', 'interpreted', 'absorbed', 'captured', 'conquered', 'embraced',
    'encountered', 'tackled', 'overcame', 'savored', 'treasured', 'cherished',
    'apprehended', 'realized', 'grasped', 'underwent', 'commemorated', 'marked',
    'recorded', 'documented', 'navigated', 'traversed', 'pioneered', 'ventured',
    'delved into', 'submerged in', 'tried', 'attempted', 'experimented', 'tested'
]


def generate_random_post():
    template = random.choice(post_templates)
    adj = random.choice(adjectives)
    noun = random.choice(nouns)
    verb = random.choice(verbs)
    place = random.choice(places)
    event = random.choice(events)
    positive_phrase = random.choice(positive_phrases)
    negative_phrase = random.choice(negative_phrases)
    neutral_phrase = random.choice(neutral_phrases)

    post = template.format(adj=adj, noun=noun, verb=verb, place=place, event=event,
                          positive_phrase=positive_phrase, negative_phrase=negative_phrase,
                          neutral_phrase=neutral_phrase)
    return post


def generate_random_comment():
    template = random.choice(comment_templates)
    adj = random.choice(adjectives)
    noun = random.choice(nouns)
    verb = random.choice(verbs)
    place = random.choice(places)
    event = random.choice(events)
    positive_phrase = random.choice(positive_phrases)
    negative_phrase = random.choice(negative_phrases)
    neutral_phrase = random.choice(neutral_phrases)

    comment = template.format(adj=adj, noun=noun, verb=verb, place=place, event=event,
                              positive_phrase=positive_phrase, negative_phrase=negative_phrase,
                              neutral_phrase=neutral_phrase)
    return comment

def generate_interactions():
    interaction_id = random.randint(10000, 99999)
    interaction = {
            'interaction_id': interaction_id,
            'user1_id': random.randint(1000, 9999),
            'user2_id': random.randint(1000, 9999),
            'action': random.choice(['like', 'comment', 'share']),
            'timestamp': random.randint(10000000, 99999999)
        }
    return interaction
    
def generate_users():
    user_id = random.randint(1000, 9999)
    user = {
            'user_id': user_id,
            'user_name': f'User {user_id}',
            'user_age': random.randint(18, 60),
            'user_location': random.choice(places)
        }
    return user

def write_to_kinesis(data):
    client = boto3.client('kinesis')
    response = client.put_record(
        StreamName='mock_server_data',
        Data=data,
        PartitionKey=str(uuid.uuid4()))
    print(response)

def lambda_handler(event, context):
    result = []
    for i in range(1000):
        id = ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(16))
        post = generate_random_post()
        comment = generate_random_comment()
        interactions = generate_interactions(2)
        users = generate_users(2)
        data = {
            'id': id,
            'post': post,
            'comment': comment,
            'interactions': interactions,
            'users': users,
            'timestamp': str(datetime.datetime.now())
        }
        result.append(data)
    
    data_string = json.dumps(result, indent=2, default=str)
    
    write_to_kinesis(data_string)
    
    return {
        'status': 200,
        'payload': 'Loaded the data to Kinesis stream successfully!!'
    }
    
