makeFighter = (firstName, lastName, dob, country, heightInCm, reachInCm, weightInLb) -> {
    firstName, lastName, dOB: dob, country, heightInCm, reachInCm, weightInLb
}

newData = () -> [
    makeFighter('Anderson', 'Silva', new Date('1975-04-14'), 'Brazil', 188, 197, 185)
    makeFighter('Wanderlei', 'Silva', new Date('1976-07-02'), 'Brazil', 180, 188, 204)
    makeFighter('Jon', 'Jones', new Date('1987-07-19'), 'USA', 193, 215, 205)
    makeFighter('Cain', 'Velasquez', new Date('1982-07-28'), 'USA', 185, 196, 240)
]

data = {
    newData
    makeFighter
    fighters: newData()

    # I keep telling my brother to drop out of residency and start his MMA carreer
    # before it's too late
    newFighter: () -> makeFighter('Guilherme', 'Duarte', new Date('1987-03-14'), 'Brazil', 180, 188, 175)
}

data.cntFighters = data.fighters.length

module.exports = data
